use crate::slideshow::FontAssets;
use crate::slideshow::SlideState;
use crate::slideshow::animation::{PunkJitter, SlamEntrance};
use crate::slideshow::code_view::{TokenKind, spawn_styled_code_block};
use crate::slideshow::diagrams::*;
use crate::theme::colors::*;
use crate::theme::geometry::*;
use crate::theme::typography::*;
use bevy::prelude::*;
use bevy::state::state_scoped::DespawnOnExit;

pub struct CalloutCard {
    pub icon: &'static str,
    pub title: &'static str,
    pub text: &'static str,
    pub is_alert: bool,
}

pub fn spawn_callout_card(
    parent: &mut ChildSpawnerCommands,
    font_assets: &FontAssets,
    card: CalloutCard,
) {
    let border_color = if card.is_alert {
        BorderColor {
            left: P5_RED,
            top: P5_WHITE,
            right: P5_WHITE,
            bottom: P5_WHITE,
        }
    } else {
        BorderColor {
            left: P5_WHITE,
            top: P5_WHITE,
            right: P5_RED,
            bottom: P5_WHITE,
        }
    };
    let icon_color = if card.is_alert { P5_RED } else { P5_WHITE };
    let title_color = if card.is_alert { P5_RED } else { P5_WHITE };

    parent
        .spawn((
            Node {
                flex_direction: FlexDirection::Column,
                padding: UiRect::all(Val::Px(12.0)),
                row_gap: Val::Px(4.0),
                border: UiRect {
                    left: if card.is_alert {
                        Val::Px(5.0)
                    } else {
                        Val::Px(2.0)
                    },
                    top: Val::Px(2.0),
                    right: if card.is_alert {
                        Val::Px(2.0)
                    } else {
                        Val::Px(5.0)
                    },
                    bottom: Val::Px(2.0),
                },
                ..default()
            },
            BackgroundColor(P5_BLACK),
            border_color,
            PunkJitter::new(0.0, 0.012),
        ))
        .with_children(|c| {
            c.spawn(Node {
                flex_direction: FlexDirection::Row,
                align_items: AlignItems::Center,
                column_gap: Val::Px(6.0),
                ..default()
            })
            .with_children(|h| {
                h.spawn((
                    Text::new(card.icon),
                    TextFont::from_font_size(FONT_BODY_ICON).with_font(font_assets.symbols.clone()),
                    TextColor(icon_color),
                ));
                h.spawn((
                    Text::new(card.title),
                    TextFont::from_font_size(FONT_BODY_LG)
                        .with_font(font_assets.sans_heavy.clone()),
                    TextColor(title_color),
                ));
            });
            c.spawn((
                Text::new(card.text),
                TextFont::from_font_size(FONT_CAPTION).with_font(font_assets.sans.clone()),
                TextColor(P5_OFF_WHITE),
            ));
        });
}

pub fn spawn_slide_frame(
    commands: &mut Commands,
    font_assets: &FontAssets,
    state: SlideState,
    tag: &'static str,
    title: &'static str,
    build_body: impl FnOnce(&mut ChildSpawnerCommands, &FontAssets),
) {
    commands
        .spawn((
            Node {
                position_type: PositionType::Absolute,
                width: Val::Percent(100.0),
                height: Val::Percent(100.0),
                padding: UiRect {
                    left: Val::Px(50.0),
                    right: Val::Px(50.0),
                    top: Val::Px(55.0),
                    bottom: Val::Px(50.0),
                },
                flex_direction: FlexDirection::Column,
                justify_content: JustifyContent::SpaceBetween,
                align_items: AlignItems::FlexStart,
                ..default()
            },
            Transform::default(),
            Visibility::default(),
            InheritedVisibility::default(),
            DespawnOnExit(state),
        ))
        .with_children(|slide| {
            // --- Header Section ---
            slide
                .spawn((
                    Node {
                        flex_direction: FlexDirection::Column,
                        align_items: AlignItems::FlexStart,
                        row_gap: Val::Px(6.0),
                        ..default()
                    },
                    Transform::default(),
                    Visibility::default(),
                    InheritedVisibility::default(),
                    SlamEntrance::new(Vec2::new(-240.0, 100.0), -0.2, 0.0),
                ))
                .with_children(|header| {
                    // Category Ribbon with Tag
                    header
                        .spawn((
                            Node {
                                padding: UiRect::axes(Val::Px(12.0), Val::Px(4.0)),
                                border: UiRect::all(Val::Px(2.0)),
                                ..default()
                            },
                            BackgroundColor(P5_RED),
                            BorderColor::all(P5_WHITE),
                            rot_counter(),
                            PunkJitter::new(-0.038, 0.015),
                        ))
                        .with_children(|tag_node| {
                            tag_node.spawn((
                                Text::new(tag),
                                TextFont::from_font_size(FONT_BODY_SM)
                                    .with_font(font_assets.display.clone()),
                                TextColor(P5_WHITE),
                            ));
                        });

                    // Title with Crimson Drop-Shadow Cutout
                    header
                        .spawn((
                            Node {
                                padding: UiRect::axes(Val::Px(16.0), Val::Px(5.0)),
                                border: UiRect {
                                    left: Val::Px(3.0),
                                    top: Val::Px(3.0),
                                    right: Val::Px(6.0),
                                    bottom: Val::Px(6.0),
                                },
                                ..default()
                            },
                            BackgroundColor(P5_BLACK),
                            BorderColor {
                                left: P5_WHITE,
                                top: P5_WHITE,
                                right: P5_RED,
                                bottom: P5_RED,
                            },
                            rot_subtle(),
                        ))
                        .with_children(|t| {
                            t.spawn((
                                Text::new(title),
                                TextFont::from_font_size(FONT_HEADING_XL)
                                    .with_font(font_assets.display.clone()),
                                TextColor(P5_WHITE),
                            ));
                        });
                });

            build_body(slide, font_assets);

            // Bottom spacer to ensure room for HUD
            slide.spawn(Node {
                height: Val::Px(20.0),
                ..default()
            });
        });
}

#[allow(clippy::too_many_arguments)]
pub fn spawn_slide_scaffold(
    commands: &mut Commands,
    font_assets: &FontAssets,
    state: SlideState,
    tag: &'static str,
    title: &'static str,
    code_filename: &'static str,
    code_badge: &'static str,
    code_lines: Vec<Vec<(&'static str, TokenKind)>>,
    build_right: impl FnOnce(&mut ChildSpawnerCommands, &FontAssets),
) {
    spawn_slide_frame(commands, font_assets, state, tag, title, |slide, fonts| {
        slide
            .spawn((
                Node {
                    width: Val::Percent(100.0),
                    flex_direction: FlexDirection::Row,
                    justify_content: JustifyContent::SpaceBetween,
                    align_items: AlignItems::Center,
                    column_gap: Val::Px(24.0),
                    ..default()
                },
                Transform::default(),
                Visibility::default(),
                InheritedVisibility::default(),
            ))
            .with_children(|row| {
                // Left: Code or Terminal container wrapper (Scrollable!)
                row.spawn((
                    Node {
                        flex_direction: FlexDirection::Column,
                        flex_grow: 1.0,
                        max_width: Val::Px(760.0),
                        ..default()
                    },
                    SlamEntrance::new(Vec2::new(-300.0, 40.0), -0.15, 0.08),
                ))
                .with_children(|code_wrapper| {
                    spawn_styled_code_block(
                        code_wrapper,
                        code_filename,
                        code_badge,
                        code_lines,
                        Some(fonts.monospace.clone()),
                    );
                });

                // Right: Tactical Persona 5 Callout Cards & Visual Diagrams
                row.spawn((
                    Node {
                        flex_direction: FlexDirection::Column,
                        row_gap: Val::Px(10.0),
                        width: Val::Px(380.0),
                        ..default()
                    },
                    rot_counter(),
                    SlamEntrance::new(Vec2::new(320.0, 60.0), 0.2, 0.14),
                ))
                .with_children(|side| {
                    build_right(side, fonts);
                });
            });
    });
}

// =========================================================================
// SLIDE 2: SYNCHRONOUS WITH ATOMICS
// =========================================================================
pub fn spawn_sync_atomics_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    let code_lines = vec![
        vec![(
            "// First commit: synchronous loop design with atomics",
            TokenKind::Comment,
        )],
        vec![
            ("pub struct ", TokenKind::Keyword),
            ("RingBuffer", TokenKind::Type),
            ("<T, const N: ", TokenKind::Plain),
            ("usize", TokenKind::Type),
            ("> {", TokenKind::Plain),
        ],
        vec![
            ("    buffer: ", TokenKind::Plain),
            ("Box", TokenKind::Type),
            ("<[", TokenKind::Plain),
            ("UnsafeCell", TokenKind::Type),
            ("<", TokenKind::Plain),
            ("MaybeUninit", TokenKind::Type),
            ("<T>>; N]>,", TokenKind::Plain),
        ],
        vec![
            ("    capacity: ", TokenKind::Plain),
            ("usize", TokenKind::Type),
            (",", TokenKind::Plain),
        ],
        vec![
            ("    size: ", TokenKind::Plain),
            ("AtomicUsize", TokenKind::Type),
            (",      ", TokenKind::Plain),
            ("// Shared counter", TokenKind::Comment),
        ],
        vec![
            ("    read_idx: ", TokenKind::Plain),
            ("AtomicUsize", TokenKind::Type),
            (",  ", TokenKind::Plain),
            ("// Read cursor", TokenKind::Comment),
        ],
        vec![
            ("    write_idx: ", TokenKind::Plain),
            ("AtomicUsize", TokenKind::Type),
            (", ", TokenKind::Plain),
            ("// Write cursor", TokenKind::Comment),
        ],
        vec![("}", TokenKind::Plain)],
        vec![("", TokenKind::Plain)],
        vec![("// Naive write implementation:", TokenKind::Comment)],
        vec![
            ("pub fn ", TokenKind::Keyword),
            ("write", TokenKind::Function),
            ("(&self, v: T) -> ", TokenKind::Plain),
            ("Result", TokenKind::Type),
            ("<(), ()> {", TokenKind::Plain),
        ],
        vec![("    loop {", TokenKind::Plain)],
        vec![
            ("        if self.", TokenKind::Plain),
            ("is_full", TokenKind::Function),
            ("() { return ", TokenKind::Plain),
            ("Err", TokenKind::Type),
            ("(()); }", TokenKind::Plain),
        ],
        vec![
            ("        let idx = self.write_idx.", TokenKind::Plain),
            ("load", TokenKind::Function),
            ("(", TokenKind::Plain),
            ("Ordering", TokenKind::Type),
            ("::Acquire);", TokenKind::Plain),
        ],
        vec![
            ("        if self.write_idx.", TokenKind::Plain),
            ("compare_exchange_weak", TokenKind::Function),
            ("(", TokenKind::Plain),
        ],
        vec![(
            "            idx, (idx + 1) % self.capacity, AcqRel, Relaxed",
            TokenKind::Plain,
        )],
        vec![("        ).is_err() { continue; }", TokenKind::Plain)],
        vec![(
            "        unsafe { self.buffer[idx].get().write(MaybeUninit::new(v)) };",
            TokenKind::Plain,
        )],
        vec![
            ("        self.size.", TokenKind::Plain),
            ("fetch_add", TokenKind::Function),
            ("(1, Ordering::SeqCst);", TokenKind::Plain),
        ],
        vec![("        break;", TokenKind::Plain)],
        vec![("    }", TokenKind::Plain)],
        vec![("    Ok(())", TokenKind::Plain)],
        vec![("}", TokenKind::Plain)],
        vec![("", TokenKind::Plain)],
        vec![(
            "// Test run: cargo test -- --nocapture (12 threads)",
            TokenKind::Comment,
        )],
        vec![("// write enter (x6) -> write exit (x6)", TokenKind::Comment)],
        vec![(
            "// read enter  (x6) -> [DEADLOCK: reads never exit!]",
            TokenKind::Comment,
        )],
    ];

    spawn_slide_scaffold(
        &mut commands,
        &font_assets,
        SlideState::MutexBottleneck,
        "/// EXHIBIT A // FIRST COMMIT (b0f8f05d) ///",
        "SYNCHRONOUS WITH ATOMICS",
        "naive_ring_buffer.rs",
        "RUST // FIRST ATTEMPT",
        code_lines,
        |side, fonts| {
            spawn_flow_diagram(
                side,
                fonts.sans.clone(),
                "WRITE THREAD (LIVELOCK POINT)",
                P5_RED,
                &[
                    "Check Full",
                    "Get Idx",
                    "CAS Crsr",
                    "Write Val",
                    "Bump Size",
                ],
                Some(4),
            );
            spawn_flow_diagram(
                side,
                fonts.sans.clone(),
                "READ THREAD (BLOCKED POINT)",
                P5_WHITE,
                &[
                    "Check Empty",
                    "Get Idx",
                    "CAS Crsr",
                    "Read Val",
                    "Decr Size",
                ],
                Some(0),
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "!",
                    title: "LIVELOCK MECHANISM",
                    text: "Full and empty depend on size, but size is updated at the END of each operation. All 6 writers exit while 6 readers deadlock forever.",
                    is_alert: true,
                },
            );
        },
    );
}

// =========================================================================
// SLIDE 3: THE SIZE VARIABLE RACE
// =========================================================================
pub fn spawn_size_focus_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    let code_lines = vec![
        vec![
            ("pub fn ", TokenKind::Keyword),
            ("read", TokenKind::Function),
            ("(&self) -> ", TokenKind::Plain),
            ("Result", TokenKind::Type),
            ("<T, ()> {", TokenKind::Plain),
        ],
        vec![("    let rr: T;", TokenKind::Plain)],
        vec![("    loop {", TokenKind::Plain)],
        vec![
            ("        if self.", TokenKind::Plain),
            ("is_empty", TokenKind::Function),
            ("() { return ", TokenKind::Plain),
            ("Err", TokenKind::Type),
            ("(()); }", TokenKind::Plain),
        ],
        vec![
            ("        let idx = self.read_idx.", TokenKind::Plain),
            ("load", TokenKind::Function),
            ("(", TokenKind::Plain),
            ("Ordering", TokenKind::Type),
            ("::Acquire);", TokenKind::Plain),
        ],
        vec![("        let r = self.buffer[idx].get();", TokenKind::Plain)],
        vec![
            ("        if self.read_idx.", TokenKind::Plain),
            ("compare_exchange_weak", TokenKind::Function),
            ("(", TokenKind::Plain),
        ],
        vec![(
            "            idx, (idx + 1) % self.capacity, AcqRel, Relaxed",
            TokenKind::Plain,
        )],
        vec![("        ).is_err() { continue; }", TokenKind::Plain)],
        vec![
            ("        self.size.", TokenKind::Plain),
            ("fetch_sub", TokenKind::Function),
            ("(1, Ordering::AcqRel); ", TokenKind::Plain),
            ("// ⚠️ Race condition!", TokenKind::Alert),
        ],
        vec![
            ("        rr = unsafe { r.", TokenKind::Plain),
            ("read", TokenKind::Function),
            ("().assume_init() };", TokenKind::Plain),
        ],
        vec![("        break;", TokenKind::Plain)],
        vec![("    }", TokenKind::Plain)],
        vec![("    Ok(rr)", TokenKind::Plain)],
        vec![("}", TokenKind::Plain)],
        vec![("", TokenKind::Plain)],
        vec![(
            "// LLDB Inspection of hung process (Process 6838):",
            TokenKind::Comment,
        )],
        vec![("// (lldb) p rbc", TokenKind::Comment)],
        vec![("// strong=6, weak=0, capacity=16384", TokenKind::Comment)],
        vec![(
            "// size = { v = { value = 18446744073687878303 } } // usize::MAX underflow!",
            TokenKind::Alert,
        )],
        vec![("// read_idx = 13667, write_idx = 0", TokenKind::Comment)],
    ];

    spawn_slide_scaffold(
        &mut commands,
        &font_assets,
        SlideState::SizeFocus,
        "/// EXHIBIT B // RACING ON SIZE ///",
        "THE SIZE VARIABLE RACE",
        "naive_read.rs",
        "RUST // LLDB TRACE",
        code_lines,
        |side, fonts| {
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// ATOMIC COUNTER UNDERFLOW ///",
                &[
                    ("size = 0 ", P5_WHITE),
                    ("──fetch_sub(1)──▶ ", P5_RED),
                    ("18,446,744,073,709,551,615", P5_BRIGHT_RED),
                ],
            );
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// 64-BIT INTEGER CEILING ///",
                &[
                    ("usize::MAX ", P5_WHITE),
                    ("= ", P5_MUTED),
                    ("2^64 - 1 (Wraparound!)", P5_GOLD),
                ],
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "!",
                    title: "PHANTOM SATURATION",
                    text: "Because size wrapped to usize::MAX, readers believe the ring is 100% full and spin endlessly attempting to read uninitialized memory.",
                    is_alert: true,
                },
            );
        },
    );
}

// =========================================================================
// SLIDE 4: STATE AMBIGUITY
// =========================================================================
pub fn spawn_state_ambiguity_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    let code_lines = vec![
        vec![(
            "// Removing size removes underflow, but creates ambiguity:",
            TokenKind::Comment,
        )],
        vec![(
            "// When read_idx == write_idx, is buffer FULL or EMPTY?",
            TokenKind::Comment,
        )],
        vec![("", TokenKind::Plain)],
        vec![(
            "// Solution: Waste 1 slot to uniquely distinguish boundaries:",
            TokenKind::Comment,
        )],
        vec![
            ("pub fn ", TokenKind::Keyword),
            ("is_full", TokenKind::Function),
            ("(&self) -> ", TokenKind::Plain),
            ("bool", TokenKind::Type),
            (" {", TokenKind::Plain),
        ],
        vec![
            ("    (self.write_idx.", TokenKind::Plain),
            ("load", TokenKind::Function),
            ("(Ordering::Acquire) + 1) % self.capacity", TokenKind::Plain),
        ],
        vec![
            ("        == self.read_idx.", TokenKind::Plain),
            ("load", TokenKind::Function),
            ("(Ordering::Acquire)", TokenKind::Plain),
        ],
        vec![("}", TokenKind::Plain)],
        vec![("", TokenKind::Plain)],
        vec![
            ("pub fn ", TokenKind::Keyword),
            ("is_empty", TokenKind::Function),
            ("(&self) -> ", TokenKind::Plain),
            ("bool", TokenKind::Type),
            (" {", TokenKind::Plain),
        ],
        vec![
            ("    self.write_idx.", TokenKind::Plain),
            ("load", TokenKind::Function),
            ("(Ordering::Acquire)", TokenKind::Plain),
        ],
        vec![
            ("        == self.read_idx.", TokenKind::Plain),
            ("load", TokenKind::Function),
            ("(Ordering::Acquire)", TokenKind::Plain),
        ],
        vec![("}", TokenKind::Plain)],
    ];

    spawn_slide_scaffold(
        &mut commands,
        &font_assets,
        SlideState::StateAmbiguity,
        "/// EXHIBIT C // ELIMINATING SIZE ///",
        "STATE AMBIGUITY",
        "ring_boundaries.rs",
        "RUST // BOUNDARY CHECK",
        code_lines,
        |side, fonts| {
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// EMPTY STATE: HEAD == TAIL ///",
                &[
                    ("write_idx.load() ", P5_WHITE),
                    ("== ", P5_RED),
                    ("read_idx.load()", P5_WHITE),
                ],
            );
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// FULL STATE: WASTE-A-SLOT ///",
                &[
                    ("(write_idx + 1) % capacity ", P5_WHITE),
                    ("== ", P5_RED),
                    ("read_idx", P5_WHITE),
                ],
            );
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// ALTERNATIVE: 2X INDEX LAPS ///",
                &[
                    ("(tail - head) ", P5_MUTED),
                    (">= ", P5_MUTED),
                    ("capacity (Doubled Array)", P5_LIGHT_GREY),
                ],
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "★",
                    title: "WASTE-A-SLOT ADVANTAGE",
                    text: "Sacrificing exactly 1 element of capacity uniquely distinguishes full from empty with zero extra synchronization or 2x array mirroring.",
                    is_alert: false,
                },
            );
        },
    );
}

// =========================================================================
// SLIDE 5: ATOMICBOOL IS NOT ENOUGH
// =========================================================================
pub fn spawn_bool_rep_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    let code_lines = vec![
        vec![
            ("struct ", TokenKind::Keyword),
            ("Slot", TokenKind::Type),
            ("<T> {", TokenKind::Plain),
        ],
        vec![
            ("    data: ", TokenKind::Plain),
            ("UnsafeCell", TokenKind::Type),
            ("<", TokenKind::Plain),
            ("MaybeUninit", TokenKind::Type),
            ("<T>>,", TokenKind::Plain),
        ],
        vec![
            ("    initialized: ", TokenKind::Plain),
            ("AtomicBool", TokenKind::Type),
            (", ", TokenKind::Plain),
            ("// Boolean readiness flag", TokenKind::Comment),
        ],
        vec![("}", TokenKind::Plain)],
        vec![("", TokenKind::Plain)],
        vec![(
            "// Writer marks true; Reader marks false:",
            TokenKind::Comment,
        )],
        vec![
            ("if !self.buffer[idx].initialized.", TokenKind::Plain),
            ("load", TokenKind::Function),
            ("(Ordering::Acquire) {", TokenKind::Plain),
        ],
        vec![
            ("    continue; ", TokenKind::Plain),
            ("// Reader spins waiting for writer", TokenKind::Comment),
        ],
        vec![("}", TokenKind::Plain)],
        vec![
            (
                "rr = unsafe { self.buffer[idx].data.get().",
                TokenKind::Plain,
            ),
            ("read", TokenKind::Function),
            ("().assume_init() };", TokenKind::Plain),
        ],
        vec![
            ("self.buffer[idx].initialized.", TokenKind::Plain),
            ("store", TokenKind::Function),
            ("(false, Ordering::Release);", TokenKind::Plain),
        ],
        vec![("", TokenKind::Plain)],
        vec![(
            "// Fails when fast producers lap slow consumers!",
            TokenKind::Comment,
        )],
    ];

    spawn_slide_scaffold(
        &mut commands,
        &font_assets,
        SlideState::BoolRep,
        "/// EXHIBIT D // MEMORY READINESS ///",
        "ATOMICBOOL IS NOT ENOUGH",
        "slot_bool.rs",
        "RUST // DATA RACE",
        code_lines,
        |side, fonts| {
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// 2-STATE LAPPING INVERSION ///",
                &[
                    ("false ", P5_MUTED),
                    ("──▶ ", P5_RED),
                    ("true ", P5_WHITE),
                    ("──▶ ", P5_RED),
                    ("false ", P5_MUTED),
                    ("──▶ ", P5_RED),
                    ("true (LAPPED!)", P5_BRIGHT_RED),
                ],
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "!",
                    title: "THE LAPPING DISASTER",
                    text: "Fast producers lap slow consumers around the ring, flipping initialized back to true. The reader wakes up and reads corrupted data from the next generation.",
                    is_alert: true,
                },
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "◆",
                    title: "NON-MONOTONIC HAZARD",
                    text: "Boolean state variables cannot distinguish Generation N from Generation N+1. Monotonic turn sequences are strictly required.",
                    is_alert: false,
                },
            );
        },
    );
}

// =========================================================================
// SLIDE 6: MANUAL MEMORY TRACKING
// =========================================================================
pub fn spawn_mem_init_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    let code_lines = vec![
        vec![(
            "// Replace boolean flag with generational sequence stamp:",
            TokenKind::Comment,
        )],
        vec![
            ("struct ", TokenKind::Keyword),
            ("Slot", TokenKind::Type),
            ("<T> {", TokenKind::Plain),
        ],
        vec![
            ("    data: ", TokenKind::Plain),
            ("UnsafeCell", TokenKind::Type),
            ("<", TokenKind::Plain),
            ("MaybeUninit", TokenKind::Type),
            ("<T>>,", TokenKind::Plain),
        ],
        vec![
            ("    stamp: ", TokenKind::Plain),
            ("AtomicUsize", TokenKind::Type),
            (", ", TokenKind::Plain),
            ("// Monotonic turn tracker", TokenKind::Comment),
        ],
        vec![("}", TokenKind::Plain)],
        vec![("", TokenKind::Plain)],
        vec![(
            "// Dynamic allocation prevents stack overflow in tests:",
            TokenKind::Comment,
        )],
        vec![
            ("let buffer: ", TokenKind::Plain),
            ("Box", TokenKind::Type),
            ("<[Slot<T>]> = (0..capacity)", TokenKind::Plain),
        ],
        vec![("    .map(|_| Slot {", TokenKind::Plain)],
        vec![(
            "        data: UnsafeCell::new(MaybeUninit::uninit()),",
            TokenKind::Plain,
        )],
        vec![("        stamp: AtomicUsize::new(0),", TokenKind::Plain)],
        vec![("    })", TokenKind::Plain)],
        vec![("    .collect();", TokenKind::Plain)],
    ];

    spawn_slide_scaffold(
        &mut commands,
        &font_assets,
        SlideState::MemInit,
        "/// EXHIBIT E // MEMORY SEQUENCING ///",
        "MANUAL MEMORY TRACKING",
        "slot_stamp.rs",
        "RUST // GENERATIONS",
        code_lines,
        |side, fonts| {
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// METADATA LOCALITY CALCULATION ///",
                &[
                    ("bitmap_words ", P5_WHITE),
                    ("= ", P5_MUTED),
                    ("capacity / 64", P5_CYAN),
                ],
            );
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// EMBEDDED GENERATION STAMP ///",
                &[
                    ("Slot<T> { data, stamp } ", P5_WHITE),
                    ("⟹ ", P5_MUTED),
                    ("1 Cache Line Hit", P5_GOLD),
                ],
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "⚡",
                    title: "STACK OVERFLOW RESOLUTION",
                    text: "std::array::from_fn temporarily allocated on the thread stack, causing a fatal stack overflow. Box with map/collect allocates directly on the heap.",
                    is_alert: false,
                },
            );
        },
    );
}

// =========================================================================
// SLIDE 7: THE ABA PROBLEM RESOLUTION
// =========================================================================
pub fn spawn_aba_problem_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    let code_lines = vec![
        vec![(
            "// Dmitry Vyukov MPMC turn-stamp algorithm:",
            TokenKind::Comment,
        )],
        vec![
            ("pub fn ", TokenKind::Keyword),
            ("write", TokenKind::Function),
            ("(&self, v: T) -> ", TokenKind::Plain),
            ("Result", TokenKind::Type),
            ("<(), ()> {", TokenKind::Plain),
        ],
        vec![
            ("    let mut head = self.write_idx.", TokenKind::Plain),
            ("load", TokenKind::Function),
            ("(Ordering::Relaxed);", TokenKind::Plain),
        ],
        vec![("    loop {", TokenKind::Plain)],
        vec![(
            "        let node = &self.buffer[head & (self.capacity - 1)];",
            TokenKind::Plain,
        )],
        vec![
            ("        let stamp = node.stamp.", TokenKind::Plain),
            ("load", TokenKind::Function),
            ("(Ordering::Acquire);", TokenKind::Plain),
        ],
        vec![(
            "        let diff = stamp as isize - head as isize;",
            TokenKind::Plain,
        )],
        vec![
            ("        if diff == 0 { ", TokenKind::Plain),
            ("// Slot ready for this write turn!", TokenKind::Comment),
        ],
        vec![(
            "            if self.write_idx.compare_exchange_weak(",
            TokenKind::Plain,
        )],
        vec![(
            "                head, head + 1, Relaxed, Relaxed",
            TokenKind::Plain,
        )],
        vec![("            ).is_ok() {", TokenKind::Plain)],
        vec![(
            "                unsafe { node.data.get().write(MaybeUninit::new(v)) };",
            TokenKind::Plain,
        )],
        vec![(
            "                node.stamp.store(head + 1, Ordering::Release);",
            TokenKind::Plain,
        )],
        vec![("                return Ok(());", TokenKind::Plain)],
        vec![("            }", TokenKind::Plain)],
        vec![("        }", TokenKind::Plain)],
        vec![("    }", TokenKind::Plain)],
        vec![("}", TokenKind::Plain)],
    ];

    spawn_slide_scaffold(
        &mut commands,
        &font_assets,
        SlideState::AbaProblem,
        "/// EXHIBIT F // RETHINKING STATES ///",
        "THE ABA PROBLEM RESOLUTION",
        "vyukov_mpmc.rs",
        "RUST // TURN-STAMP",
        code_lines,
        |side, fonts| {
            spawn_ring_buffer_diagram(
                side,
                fonts.monospace.clone(),
                "VYUKOV MPMC RING BUFFER (N=4)",
                vec![
                    SlotState {
                        label: "Slot 0",
                        value: "0xAA",
                        state: "stamp: 4",
                        is_highlighted: true,
                        is_error: false,
                    },
                    SlotState {
                        label: "Slot 1",
                        value: "0xBB",
                        state: "stamp: 1",
                        is_highlighted: false,
                        is_error: false,
                    },
                    SlotState {
                        label: "Slot 2",
                        value: "0xCC",
                        state: "stamp: 2",
                        is_highlighted: false,
                        is_error: false,
                    },
                    SlotState {
                        label: "Slot 3",
                        value: "0x00",
                        state: "stamp: 3",
                        is_highlighted: false,
                        is_error: false,
                    },
                ],
                0,
                3,
            );
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// TURN-STAMP INVARIANT ///",
                &[
                    ("diff ", P5_WHITE),
                    ("= ", P5_MUTED),
                    ("stamp - head", P5_CYAN),
                    (" (0=write, 1=read)", P5_MUTED),
                ],
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "★",
                    title: "ABA IMMUNITY",
                    text: "Vyukov's stamp increments monotonically (head + 1). A thread cannot write unless stamp == head, rendering ABA mathematically impossible.",
                    is_alert: false,
                },
            );
        },
    );
}

// =========================================================================
// SLIDE 8: TESTING WITH LOOM
// =========================================================================
pub fn spawn_thread_safety_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    let code_lines = vec![
        vec![(
            "INFO iter{68005}:thread{id=0}: loom::rt::execution: ~~~ THREAD 0 ~~~",
            TokenKind::Comment,
        )],
        vec![(
            "TRACE loom::rt::atomic: Atomic::load state=Ref(7) ordering=Acquire",
            TokenKind::Plain,
        )],
        vec![(
            "TRACE loom::rt::atomic: Atomic::rmw state=Ref(7) success=AcqRel failure=Relaxed",
            TokenKind::Plain,
        )],
        vec![(
            "TRACE loom::rt::atomic: Atomic::store state=Ref(5) ordering=Release",
            TokenKind::Plain,
        )],
        vec![(
            "INFO iter{68005}:thread{id=1}: loom::rt::execution: ~~~ THREAD 1 ~~~",
            TokenKind::Comment,
        )],
        vec![(
            "TRACE loom::rt: yield_now thread=Id(2) switch=true",
            TokenKind::Plain,
        )],
        vec![(
            "TRACE loom::rt::atomic: Atomic::load state=Ref(6) ordering=Acquire",
            TokenKind::Plain,
        )],
        vec![(
            "// Exhaustive permutation explores 68,000+ atomic interleavings!",
            TokenKind::Comment,
        )],
        vec![(
            "// Proves zero data races or invalid state transitions exist!",
            TokenKind::Comment,
        )],
    ];

    spawn_slide_scaffold(
        &mut commands,
        &font_assets,
        SlideState::ThreadSafety,
        "/// EXHIBIT G // FORMAL VERIFICATION ///",
        "TESTING WITH LOOM",
        "loom_trace.log",
        "LOOM // MODEL CHECK",
        code_lines,
        |side, fonts| {
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// LOOM PERMUTATION METRICS ///",
                &[
                    ("Schedules Explored: ", P5_MUTED),
                    ("68,005 Permutations", P5_CYAN),
                ],
            );
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// FORMAL MODEL CHECK RESULT ///",
                &[
                    ("Data Races Detected: ", P5_MUTED),
                    ("0 (VERIFIED SAFE)", P5_WHITE),
                ],
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "!",
                    title: "REPETITION IS INSUFFICIENT",
                    text: "Running cargo test 10,000 times relies on random OS scheduling. Rare memory ordering bugs only trigger 1 in 100,000 runs.",
                    is_alert: true,
                },
            );
        },
    );
}

// =========================================================================
// SLIDE 9: CRASH IN CRITICAL SECTION
// =========================================================================
pub fn spawn_recovery_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    let code_lines = vec![
        vec![(
            "// Lockless slot advancement is a 3-step sequence:",
            TokenKind::Comment,
        )],
        vec![(
            "// 1. Claim slot cursor via atomic CAS:",
            TokenKind::Comment,
        )],
        vec![
            ("let idx = self.write_idx.", TokenKind::Plain),
            ("compare_exchange_weak", TokenKind::Function),
            ("(...)?;", TokenKind::Plain),
        ],
        vec![("", TokenKind::Plain)],
        vec![(
            "// 2. Write payload into buffer memory:",
            TokenKind::Comment,
        )],
        vec![
            ("unsafe { self.buffer[idx].data.get().", TokenKind::Plain),
            ("write", TokenKind::Function),
            ("(MaybeUninit::new(v)) };", TokenKind::Plain),
        ],
        vec![("", TokenKind::Plain)],
        vec![(
            "// 3. Commit slot stamp to wake next reader:",
            TokenKind::Comment,
        )],
        vec![
            ("self.buffer[idx].stamp.", TokenKind::Plain),
            ("store", TokenKind::Function),
            ("(idx + 1, Ordering::Release);", TokenKind::Plain),
        ],
        vec![("", TokenKind::Plain)],
        vec![(
            "// IF THREAD CRASHES (SIGKILL/OOM/PANIC) AT STEP 2:",
            TokenKind::Comment,
        )],
        vec![(
            "// Slot stamp is NEVER committed! All subsequent threads freeze!",
            TokenKind::Comment,
        )],
    ];

    spawn_slide_scaffold(
        &mut commands,
        &font_assets,
        SlideState::Recovery,
        "/// EXHIBIT H // CRITICAL SECTION FAILURE ///",
        "CRASH IN CRITICAL SECTION",
        "stalled_slot.rs",
        "RUST // VULNERABILITY",
        code_lines,
        |side, fonts| {
            spawn_flow_diagram(
                side,
                fonts.sans.clone(),
                "CRITICAL SECTION PROGRESSION",
                P5_RED,
                &["1. CAS Index", "2. Write Value", "3. Store Stamp"],
                Some(1),
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "!",
                    title: "LOCKLESS != LOCK-FREE",
                    text: "Lock-free guarantees system-wide forward progress. Vyukov's algorithm is lockless but blocking per-slot; if a thread dies at step 2, the slot deadlocks forever.",
                    is_alert: true,
                },
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "◆",
                    title: "UNRECOVERABLE HALT",
                    text: "If a thread is killed (SIGKILL/OOM) while holding an uncommitted index, that slot is permanently orphaned without manual intervention.",
                    is_alert: false,
                },
            );
        },
    );
}

// =========================================================================
// SLIDE 10: AUTOMATING RECOVERY REINTRODUCES ABA
// =========================================================================
pub fn spawn_automate_recovery_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    let code_lines = vec![
        vec![(
            "// Attempting peer-thread recovery with retry threshold:",
            TokenKind::Comment,
        )],
        vec![("let mut retry = (0, idx);", TokenKind::Plain)],
        vec![("loop {", TokenKind::Plain)],
        vec![
            ("    if self.buffer[i].stamp.", TokenKind::Plain),
            ("load", TokenKind::Function),
            ("(Ordering::Acquire) != ridx {", TokenKind::Plain),
        ],
        vec![("        match retry {", TokenKind::Plain)],
        vec![(
            "            // If stalled for 10 retries, force-advance stamp:",
            TokenKind::Comment,
        )],
        vec![
            (
                "            (cnt, id) if cnt >= 10 && id == idx => self.",
                TokenKind::Plain,
            ),
            ("fix", TokenKind::Function),
            ("(id), // DANGER!", TokenKind::Alert),
        ],
        vec![(
            "            (cnt, id) if id == idx => retry.0 += 1,",
            TokenKind::Plain,
        )],
        vec![("            _ => retry = (0, idx),", TokenKind::Plain)],
        vec![("        }", TokenKind::Plain)],
        vec![("        continue;", TokenKind::Plain)],
        vec![("    }", TokenKind::Plain)],
        vec![("}", TokenKind::Plain)],
    ];

    spawn_slide_scaffold(
        &mut commands,
        &font_assets,
        SlideState::AutomateRecovery,
        "/// EXHIBIT I // HEALING TRAPS ///",
        "AUTOMATING RECOVERY REINTRODUCES ABA",
        "peer_recovery.rs",
        "RUST // HEALING HAZARD",
        code_lines,
        |side, fonts| {
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// LAP DISTANCE CALCULATION ///",
                &[
                    ("|floor(read/cap) - floor(stamp/cap)| ", P5_WHITE),
                    (">= ", P5_RED),
                    ("2 (Lapped)", P5_WHITE),
                ],
            );
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// ADJACENT STAMP HEURISTIC ///",
                &[
                    ("post_init ", P5_WHITE),
                    ("= ", P5_MUTED),
                    ("present + cap + 1", P5_CYAN),
                ],
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "!",
                    title: "PREEMPTION VS CRASH",
                    text: "A peer thread cannot distinguish a killed thread from an OS-preempted thread. Force-advancing a slot stamp causes the delayed thread to overwrite future data when it wakes up.",
                    is_alert: true,
                },
            );
        },
    );
}

// =========================================================================
// SLIDE 11: MEASURING CACHE CONTENTION
// =========================================================================
pub fn spawn_cache_contention_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    let code_lines = vec![
        vec![(
            "Hardware Performance Counters (Instruments on Apple Silicon):",
            TokenKind::Comment,
        )],
        vec![(
            "L1D_CACHE_MISS_LD_NONSPEC (L1 Load Misses): 239,011,001",
            TokenKind::Plain,
        )],
        vec![(
            "INST_INT_LD (Integer Load Operations):      471,024,178",
            TokenKind::Plain,
        )],
        vec![(
            "  ==> L1 Load Miss Rate: 50.74% (Severe Cache Thrashing!)",
            TokenKind::Keyword,
        )],
        vec![("", TokenKind::Plain)],
        vec![(
            "ATOMIC_OR_EXCLUSIVE_FAIL (CAS Failures):    50,024,859",
            TokenKind::Plain,
        )],
        vec![(
            "ATOMIC_OR_EXCLUSIVE_SUCC (CAS Successes):   24,452,925",
            TokenKind::Plain,
        )],
        vec![(
            "  ==> CAS Contention Failure Rate: 67.17% (2/3 CAS ops fail!)",
            TokenKind::Keyword,
        )],
    ];

    spawn_slide_scaffold(
        &mut commands,
        &font_assets,
        SlideState::CacheContention,
        "/// EXHIBIT J // HARDWARE PROFILING ///",
        "MEASURING CACHE CONTENTION",
        "instruments_counters.txt",
        "MACOS // PERF COUNTERS",
        code_lines,
        |side, fonts| {
            spawn_bar_chart(
                side,
                fonts.sans_heavy.clone(),
                "MACOS PERF COUNTERS (APPLE SILICON)",
                220.0,
                vec![
                    BarEntry {
                        label: "L1 Load Miss Rate",
                        value_text: "50.74%",
                        fraction: 0.5074,
                        color: P5_RED,
                    },
                    BarEntry {
                        label: "CAS Failure Rate",
                        value_text: "67.17%",
                        fraction: 0.6717,
                        color: P5_BRIGHT_RED,
                    },
                    BarEntry {
                        label: "IPC Instructions/Cyc",
                        value_text: "0.0306",
                        fraction: 0.05,
                        color: P5_MUTED,
                    },
                ],
            );
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// L1 MISS RATE FORMULA ///",
                &[
                    ("239M Misses ", P5_RED),
                    ("/ ", P5_MUTED),
                    ("471M Loads ", P5_WHITE),
                    ("= ", P5_MUTED),
                    ("50.74%", P5_BRIGHT_RED),
                ],
            );
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// CAS FAILURE RATE FORMULA ///",
                &[
                    ("50M Fails ", P5_RED),
                    ("/ ", P5_MUTED),
                    ("74M Total ", P5_WHITE),
                    ("= ", P5_MUTED),
                    ("67.17% (2/3 Wasted!)", P5_BRIGHT_RED),
                ],
            );
        },
    );
}

// =========================================================================
// SLIDE 12: DISASSEMBLY & CACHE LINES
// =========================================================================
pub fn spawn_disassembled_code_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    let code_lines = vec![
        vec![(
            "// Disassembly revealed read_idx & write_idx stored consecutively:",
            TokenKind::Comment,
        )],
        vec![
            ("pub struct ", TokenKind::Keyword),
            ("RingBuffer", TokenKind::Type),
            ("<T> {", TokenKind::Plain),
        ],
        vec![
            ("    read_idx: ", TokenKind::Plain),
            ("AtomicUsize", TokenKind::Type),
            (",  ", TokenKind::Plain),
            (
                "// Offset 0x00..0x08 (Shared 64-byte line)",
                TokenKind::Comment,
            ),
        ],
        vec![
            ("    write_idx: ", TokenKind::Plain),
            ("AtomicUsize", TokenKind::Type),
            (", ", TokenKind::Plain),
            (
                "// Offset 0x08..0x10 (Shared 64-byte line)",
                TokenKind::Comment,
            ),
        ],
        vec![("}", TokenKind::Plain)],
        vec![("", TokenKind::Plain)],
        vec![(
            "// MESI protocol forces invalidation on every producer write!",
            TokenKind::Comment,
        )],
        vec![(
            "// Fix: Hardware cache-line padding (64 bytes on ARM64/x86_64)",
            TokenKind::Comment,
        )],
        vec![("#[repr(align(64))]", TokenKind::Keyword)],
        vec![
            ("pub struct ", TokenKind::Keyword),
            ("CachePadded", TokenKind::Type),
            ("<T>(pub T);", TokenKind::Plain),
        ],
        vec![("", TokenKind::Plain)],
        vec![("read_idx: CachePadded<AtomicUsize>,", TokenKind::Plain)],
        vec![("write_idx: CachePadded<AtomicUsize>,", TokenKind::Plain)],
    ];

    spawn_slide_scaffold(
        &mut commands,
        &font_assets,
        SlideState::DisassembledCode,
        "/// EXHIBIT K // FALSE SHARING ///",
        "DISASSEMBLY & CACHE LINES",
        "cache_false_sharing.rs",
        "ASM // CACHE COHERENCY",
        code_lines,
        |side, fonts| {
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// CACHE LINE CONTENTION (64 BYTES) ///",
                &[
                    ("read_idx [0x00] ", P5_WHITE),
                    ("| ", P5_MUTED),
                    ("write_idx [0x08] ", P5_WHITE),
                    ("(SAME 64B LINE!)", P5_RED),
                ],
            );
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// PADDED HARDWARE ISOLATION ///",
                &[
                    ("#[repr(align(64))] ", P5_CYAN),
                    ("⟹ ", P5_MUTED),
                    ("Isolated MESI Lines", P5_WHITE),
                ],
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "!",
                    title: "FALSE SHARING",
                    text: "Because read_idx and write_idx share a 64-byte line, writing by a producer invalidates the L1 cache of the consumer, destroying throughput.",
                    is_alert: true,
                },
            );
        },
    );
}

// =========================================================================
// SLIDE 13: BRANCHLESS COMPUTATION
// =========================================================================
pub fn spawn_branchless_index_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    let code_lines = vec![
        vec![(
            "// Slow: Modulo division instruction (div / idiv takes 10-30 cycles)",
            TokenKind::Comment,
        )],
        vec![
            (
                "let next_idx = (idx + 1) % self.capacity; ",
                TokenKind::Plain,
            ),
            ("// Expensive branch", TokenKind::Comment),
        ],
        vec![("", TokenKind::Plain)],
        vec![(
            "// Fast: Power-of-two bitwise masking (Single CPU clock cycle!)",
            TokenKind::Comment,
        )],
        vec![(
            "// Precondition: capacity must be a power of two (e.g. 1024, 4096)",
            TokenKind::Comment,
        )],
        vec![("let mask = self.capacity - 1;", TokenKind::Plain)],
        vec![("", TokenKind::Plain)],
        vec![
            ("let slot_idx = idx & mask;           ", TokenKind::Plain),
            ("// Direct array mapping", TokenKind::Comment),
        ],
        vec![
            ("let next_idx = (idx + 1) & mask;     ", TokenKind::Plain),
            ("// Zero-branch wrap", TokenKind::Comment),
        ],
        vec![
            ("let lap_gen  = idx & !mask;          ", TokenKind::Plain),
            ("// Cycle generation", TokenKind::Comment),
        ],
    ];

    spawn_slide_scaffold(
        &mut commands,
        &font_assets,
        SlideState::BranchlessIndex,
        "/// EXHIBIT L // INSTRUCTION TUNING ///",
        "BRANCHLESS COMPUTATION",
        "branchless_mask.rs",
        "RUST // ZERO BRANCHES",
        code_lines,
        |side, fonts| {
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// MODULO DIVISION (10-30 CYCLES) ///",
                &[
                    ("next_idx ", P5_MUTED),
                    ("= ", P5_MUTED),
                    ("(idx + 1) % capacity (DIV/IDIV)", P5_RED),
                ],
            );
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// BITWISE MASKING (1 CLOCK CYCLE) ///",
                &[
                    ("slot_idx ", P5_CYAN),
                    ("= ", P5_MUTED),
                    ("idx & (capacity - 1)", P5_WHITE),
                ],
            );
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// GENERATIONAL LAP EXTRACTION ///",
                &[
                    ("lap_gen  ", P5_GOLD),
                    ("= ", P5_MUTED),
                    ("idx & !(capacity - 1)", P5_WHITE),
                ],
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "⚡",
                    title: "PIPELINE EFFICIENCY",
                    text: "Branchless wrapping prevents CPU branch predictor misses and pipeline stalls in the hot path, maximizing instruction throughput.",
                    is_alert: false,
                },
            );
        },
    );
}

// =========================================================================
// SLIDE 14: MEASURING TAIL LATENCY
// =========================================================================
pub fn spawn_tail_latency_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    let code_lines = vec![
        vec![(
            "// User-Level Statically Defined Tracing (USDT) probe hooks:",
            TokenKind::Comment,
        )],
        vec![(
            "limitless_probes*:::read-start  { self->read_ts = timestamp; }",
            TokenKind::Plain,
        )],
        vec![(
            "limitless_probes*:::read-done /self->read_ts/ {",
            TokenKind::Plain,
        )],
        vec![(
            "    @read_lat = quantize(timestamp - self->read_ts);",
            TokenKind::Plain,
        )],
        vec![("    self->read_ts = 0;", TokenKind::Plain)],
        vec![("}", TokenKind::Plain)],
        vec![("", TokenKind::Plain)],
        vec![(
            "limitless_probes*:::write-start { self->write_ts = timestamp; }",
            TokenKind::Plain,
        )],
        vec![(
            "limitless_probes*:::write-done /self->write_ts/ {",
            TokenKind::Plain,
        )],
        vec![(
            "    @write_lat = quantize(timestamp - self->write_ts);",
            TokenKind::Plain,
        )],
        vec![("    self->write_ts = 0;", TokenKind::Plain)],
        vec![("}", TokenKind::Plain)],
    ];

    spawn_slide_scaffold(
        &mut commands,
        &font_assets,
        SlideState::TailLatency,
        "/// EXHIBIT M // OBSERVABILITY ///",
        "MEASURING TAIL LATENCY",
        "tail_latency.d",
        "DTRACE // USDT PROBES",
        code_lines,
        |side, fonts| {
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// USDT COMPILER OVERHEAD ///",
                &[
                    ("Production Disabled: ", P5_MUTED),
                    ("1 NOP (0.0 ns)", P5_CYAN),
                ],
            );
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// DTRACE PROBE ATTACH ///",
                &[
                    ("Dynamic Instrumentation: ", P5_MUTED),
                    ("quantize(t1 - t0)", P5_WHITE),
                ],
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "◆",
                    title: "BEYOND CRITERION",
                    text: "Criterion averages hide extreme latency outliers. Real-world concurrency requires measuring tail latency (p99, p99.9).",
                    is_alert: false,
                },
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "★",
                    title: "ZERO-COST PROBES",
                    text: "USDT compiles to single NOP instructions in release builds. Enabled dynamically via DTrace with zero baseline production overhead.",
                    is_alert: false,
                },
            );
        },
    );
}

// =========================================================================
// SLIDE 15: UNBOUNDED TAIL LATENCY
// =========================================================================
pub fn spawn_unbounded_latency_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    let code_lines = vec![
        vec![(
            "$ sudo dtrace -c ./target/release/limitless -s tail_latency.d",
            TokenKind::Comment,
        )],
        vec![(
            "READ LATENCY (ns)               WRITE LATENCY (ns)",
            TokenKind::Plain,
        )],
        vec![(
            " 256 |@@@@@@@@@@@@@@@@@@ 17932   512 |@@@@@@@@@@@@@@@@ 8316",
            TokenKind::Plain,
        )],
        vec![(
            " 512 |@@@@@               4943  1024 |@@@@@            2510",
            TokenKind::Plain,
        )],
        vec![(
            "1024 |@@                  1818  8192 |                    2",
            TokenKind::Plain,
        )],
        vec![(
            "16384 |                      3 131072 |                    1",
            TokenKind::Plain,
        )],
        vec![("", TokenKind::Plain)],
        vec![(
            "Median Read:  256 ns  --> Tail Spike:  16,384 ns (64x worse!)",
            TokenKind::Keyword,
        )],
        vec![(
            "Median Write: 512 ns  --> Tail Spike: 131,072 ns (256x worse!)",
            TokenKind::Keyword,
        )],
    ];

    spawn_slide_scaffold(
        &mut commands,
        &font_assets,
        SlideState::UnboundedLatency,
        "/// EXHIBIT N // LATENCY TAIL ///",
        "UNBOUNDED TAIL LATENCY",
        "dtrace_output.txt",
        "HISTOGRAM // OUTLIERS",
        code_lines,
        |side, fonts| {
            spawn_histogram(
                side,
                fonts.monospace.clone(),
                "DTRACE LATENCY QUANTIZE HISTOGRAM",
                vec![
                    HistogramBucket {
                        label: "256 ns",
                        count: 17932,
                        max_count: 17932,
                        is_outlier: false,
                    },
                    HistogramBucket {
                        label: "512 ns",
                        count: 4943,
                        max_count: 17932,
                        is_outlier: false,
                    },
                    HistogramBucket {
                        label: "1024 ns",
                        count: 1818,
                        max_count: 17932,
                        is_outlier: false,
                    },
                    HistogramBucket {
                        label: "16.4 µs",
                        count: 3,
                        max_count: 17932,
                        is_outlier: true,
                    },
                ],
                180.0,
            );
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// READ TAIL LATENCY SPIKE ///",
                &[
                    ("16,384 ns / 256 ns ", P5_WHITE),
                    ("= ", P5_MUTED),
                    ("64x degradation!", P5_BRIGHT_RED),
                ],
            );
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// WRITE TAIL LATENCY SPIKE ///",
                &[
                    ("131,072 ns / 512 ns ", P5_WHITE),
                    ("= ", P5_MUTED),
                    ("256x degradation!", P5_BRIGHT_RED),
                ],
            );
        },
    );
}

// =========================================================================
// SLIDE 16: BACKOFF MITIGATION
// =========================================================================
pub fn spawn_backoffs_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    let code_lines = vec![
        vec![
            ("use ", TokenKind::Keyword),
            ("crossbeam_utils::Backoff;", TokenKind::Plain),
        ],
        vec![("", TokenKind::Plain)],
        vec![("let backoff = Backoff::new();", TokenKind::Plain)],
        vec![("loop {", TokenKind::Plain)],
        vec![
            ("    if self.", TokenKind::Plain),
            ("try_acquire_slot", TokenKind::Function),
            ("(idx) {", TokenKind::Plain),
        ],
        vec![
            ("        break; ", TokenKind::Plain),
            ("// Successfully claimed slot turn", TokenKind::Comment),
        ],
        vec![("    }", TokenKind::Plain)],
        vec![("    // Adaptive exponential backoff:", TokenKind::Comment)],
        vec![(
            "    // 1. Spurious spins (core::hint::spin_loop)",
            TokenKind::Comment,
        )],
        vec![(
            "    // 2. Cooperative thread yield (std::thread::yield_now)",
            TokenKind::Comment,
        )],
        vec![
            ("    backoff.", TokenKind::Plain),
            ("snooze", TokenKind::Function),
            ("();", TokenKind::Plain),
        ],
        vec![("}", TokenKind::Plain)],
    ];

    spawn_slide_scaffold(
        &mut commands,
        &font_assets,
        SlideState::Backoffs,
        "/// EXHIBIT O // CONTENTION CONTROL ///",
        "BACKOFF MITIGATION",
        "backoff_tuning.rs",
        "RUST // CROSSBEAM",
        code_lines,
        |side, fonts| {
            spawn_flow_diagram(
                side,
                fonts.sans.clone(),
                "EXPONENTIAL BACKOFF STAGES",
                P5_GOLD,
                &[
                    "1. CAS Check",
                    "2. spin_loop()",
                    "3. yield_now()",
                    "4. Snooze",
                ],
                Some(1),
            );
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// UNCONTROLLED TAIL SPIKE ///",
                &[
                    ("Tight CAS Spin: ", P5_MUTED),
                    ("131,072 ns (256x)", P5_RED),
                ],
            );
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// STABILIZED TAIL LATENCY ///",
                &[
                    ("With Exponential Backoff: ", P5_MUTED),
                    ("< 1,000 ns (p99)", P5_WHITE),
                ],
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "★",
                    title: "BUS STORM RELIEF",
                    text: "Spinning aggressively saturates the L1/L2 memory bus interconnect. Backoff throttles atomic retry frequency, bringing down tail latency outliers.",
                    is_alert: false,
                },
            );
        },
    );
}

// =========================================================================
// SLIDE 17: CORE SCALING LIMITATIONS
// =========================================================================
pub fn spawn_scaling_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    let code_lines = vec![
        vec![(
            "[Core 0] [Core 1] [Core 2] [Core 3] ---> Contending on read_idx",
            TokenKind::Plain,
        )],
        vec![("  |         |        |        |", TokenKind::Comment)],
        vec![(
            " [==== CPU INTERCONNECT MESI INVALIDATION SATURATION ====]",
            TokenKind::Keyword,
        )],
        vec![("  |         |        |        |", TokenKind::Comment)],
        vec![(
            "[Core 4] [Core 5] [Core 6] [Core 7] ---> Contending on write_idx",
            TokenKind::Plain,
        )],
        vec![("", TokenKind::Plain)],
        vec![(
            "// Result: As logical core counts increase beyond 8 cores,",
            TokenKind::Comment,
        )],
        vec![(
            "// interconnect contention causes total throughput to DECLINE!",
            TokenKind::Comment,
        )],
    ];

    spawn_slide_scaffold(
        &mut commands,
        &font_assets,
        SlideState::Scaling,
        "/// EXHIBIT P // SCALING CEILING ///",
        "CORE SCALING LIMITATIONS",
        "core_contention.txt",
        "TOPOLOGY // MESI BUS",
        code_lines,
        |side, fonts| {
            spawn_bar_chart(
                side,
                fonts.sans_heavy.clone(),
                "THROUGHPUT VS CORE COUNT",
                200.0,
                vec![
                    BarEntry {
                        label: "2 Cores",
                        value_text: "1.00x base",
                        fraction: 0.35,
                        color: P5_WHITE,
                    },
                    BarEntry {
                        label: "4 Cores",
                        value_text: "2.30x",
                        fraction: 0.85,
                        color: P5_CYAN,
                    },
                    BarEntry {
                        label: "8 Cores",
                        value_text: "2.70x PEAK",
                        fraction: 1.00,
                        color: P5_GOLD,
                    },
                    BarEntry {
                        label: "12 Cores",
                        value_text: "1.40x DROP",
                        fraction: 0.50,
                        color: P5_RED,
                    },
                    BarEntry {
                        label: "16 Cores",
                        value_text: "0.70x FAIL",
                        fraction: 0.25,
                        color: P5_BRIGHT_RED,
                    },
                ],
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "!",
                    title: "MESI BUS SATURATION",
                    text: "Each atomic write requires broadcasting invalidations across core interconnects. Beyond 8-12 cores, bus arbitration dominates execution.",
                    is_alert: true,
                },
            );
        },
    );
}

// =========================================================================
// SLIDE 18: SPSC & ARCHITECTURAL PATHS
// =========================================================================
pub fn spawn_simplification_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    let code_lines = vec![
        vec![(
            "// SPSC (Single Producer, Single Consumer) eliminates CAS entirely!",
            TokenKind::Comment,
        )],
        vec![
            ("pub fn ", TokenKind::Keyword),
            ("push", TokenKind::Function),
            ("(&self, val: T) -> ", TokenKind::Plain),
            ("Result", TokenKind::Type),
            ("<(), Full> {", TokenKind::Plain),
        ],
        vec![
            ("    let write = self.write_idx.", TokenKind::Plain),
            ("load", TokenKind::Function),
            ("(Ordering::Relaxed);", TokenKind::Plain),
        ],
        vec![(
            "    // Check local cached read index to avoid cross-core memory loads:",
            TokenKind::Comment,
        )],
        vec![(
            "    if write - self.cached_read.get() >= self.capacity {",
            TokenKind::Plain,
        )],
        vec![(
            "        self.cached_read.set(self.read_idx.load(Ordering::Acquire));",
            TokenKind::Plain,
        )],
        vec![(
            "        if write - self.cached_read.get() >= self.capacity { return Err(Full); }",
            TokenKind::Plain,
        )],
        vec![("    }", TokenKind::Plain)],
        vec![(
            "    unsafe { self.buffer[write & self.mask].write(val) };",
            TokenKind::Plain,
        )],
        vec![(
            "    self.write_idx.store(write + 1, Ordering::Release);",
            TokenKind::Plain,
        )],
        vec![("    Ok(())", TokenKind::Plain)],
        vec![("}", TokenKind::Plain)],
    ];

    spawn_slide_scaffold(
        &mut commands,
        &font_assets,
        SlideState::Simplification,
        "/// EXHIBIT Q // FUTURE HORIZONS ///",
        "SPSC & ARCHITECTURAL PATHS",
        "spsc_ring_buffer.rs",
        "RUST // ZERO CONTENTION",
        code_lines,
        |side, fonts| {
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// SPSC SYNCHRONIZATION OVERHEAD ///",
                &[
                    ("Atomic CAS Instructions: ", P5_MUTED),
                    ("0 (Zero Contention)", P5_WHITE),
                ],
            );
            spawn_math_formula(
                side,
                fonts.monospace.clone(),
                "/// MEMORY ORDERING REQUIREMENT ///",
                &[
                    ("Stores: ", P5_MUTED),
                    ("Release", P5_CYAN),
                    (" | Loads: ", P5_MUTED),
                    ("Acquire", P5_GOLD),
                ],
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "★",
                    title: "ZERO CAS (SPSC)",
                    text: "Single producer & consumer requires no CAS—only synchronized load/store with Acquire/Release. Eliminates bus contention completely.",
                    is_alert: false,
                },
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "⚡",
                    title: "CORE PINNING & CACHED INDICES",
                    text: "Pinning threads to dedicated cores keeps L1/L2 caches hot. Local index caching avoids checking cross-core atomics on every iteration.",
                    is_alert: false,
                },
            );
        },
    );
}
