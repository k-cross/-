use crate::slideshow::FontAssets;
use crate::slideshow::SlideState;
use crate::slideshow::animation::SlamEntrance;
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
            .spawn((Node {
                width: Val::Percent(100.0),
                flex_direction: FlexDirection::Row,
                justify_content: JustifyContent::SpaceBetween,
                align_items: AlignItems::Center,
                column_gap: Val::Px(24.0),
                ..default()
            },))
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
            ("// [!] Race: size underflows!", TokenKind::Alert),
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
                fonts,
                "/// ATOMIC COUNTER UNDERFLOW ///",
                &[
                    ("size = 0 ", P5_WHITE),
                    ("──fetch_sub(1)──▶ ", P5_RED),
                    ("18,446,744,073,709,551,615", P5_BRIGHT_RED),
                ],
            );
            spawn_math_formula(
                side,
                fonts,
                "/// 64-BIT INTEGER CEILING ///",
                &[
                    ("usize::MAX ", P5_WHITE),
                    ("= ", P5_MUTED),
                    ("2⁶⁴ − 1 (Wraparound!)", P5_GOLD),
                ],
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "!",
                    title: "PHANTOM DATA RACE",
                    text: "Because size wrapped to usize::MAX, is_empty() returned false. Readers believed the buffer had data and spun endlessly in CAS/read loops instead of returning Err(()).",
                    is_alert: true,
                },
            );
        },
    );
}

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
                fonts,
                "/// EMPTY STATE: HEAD == TAIL ///",
                &[
                    ("write_idx ", P5_WHITE),
                    ("= ", P5_RED),
                    ("read_idx", P5_WHITE),
                ],
            );
            spawn_math_formula(
                side,
                fonts,
                "/// FULL STATE: WASTE-A-SLOT ///",
                &[
                    ("(write_idx + 1) mod cap ", P5_WHITE),
                    ("= ", P5_RED),
                    ("read_idx", P5_WHITE),
                ],
            );
            spawn_math_formula(
                side,
                fonts,
                "/// ALTERNATIVE: 2X INDEX LAPS ///",
                &[
                    ("(tail − head) ", P5_MUTED),
                    ("≥ ", P5_MUTED),
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

pub fn spawn_bool_rep_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    let code_lines = vec![
        vec![(
            "// Without size, write_idx advances BEFORE data is written,",
            TokenKind::Comment,
        )],
        vec![(
            "// so fast readers can read uninitialized memory!",
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
            ("    initialized: ", TokenKind::Plain),
            ("AtomicBool", TokenKind::Type),
            (", ", TokenKind::Plain),
            ("// Guard each slot's readiness", TokenKind::Comment),
        ],
        vec![("}", TokenKind::Plain)],
        vec![("", TokenKind::Plain)],
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
            "// LLDB (Process 17878): read_idx == write_idx == 1977",
            TokenKind::Alert,
        )],
        vec![(
            "// Slot { initialized: true, data: 1 } -> Write threads livelock!",
            TokenKind::Alert,
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
                fonts,
                "/// 16-BYTE SLOT MEMORY LAYOUT (LLDB) ///",
                &[
                    ("data (8B) ", P5_WHITE),
                    ("+ ", P5_MUTED),
                    ("bool (1B) ", P5_CYAN),
                    ("+ ", P5_MUTED),
                    ("pad (7B)", P5_MUTED),
                ],
            );
            spawn_math_formula(
                side,
                fonts,
                "/// 2-STATE BOOLEAN CYCLE ///",
                &[
                    ("false ", P5_MUTED),
                    ("──▶ ", P5_RED),
                    ("true ", P5_WHITE),
                    ("──▶ ", P5_RED),
                    ("false ", P5_MUTED),
                    ("──▶ ", P5_RED),
                    ("true (ABA!)", P5_BRIGHT_RED),
                ],
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "!",
                    title: "UNINITIALIZED READS & LIVELOCK",
                    text: "Advancing write_idx before writing data exposes uninitialized memory. Adding AtomicBool per slot causes writers to livelock at index 1977 when check ordering races.",
                    is_alert: true,
                },
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "◆",
                    title: "NON-MONOTONIC HAZARD",
                    text: "Even after reordering full/empty checks, a 2-state boolean cannot distinguish Lap N from Lap N+1 when a reader is descheduled.",
                    is_alert: false,
                },
            );
        },
    );
}

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
                fonts,
                "/// METADATA LOCALITY CALCULATION ///",
                &[
                    ("bitmap_words ", P5_WHITE),
                    ("= ", P5_MUTED),
                    ("capacity ÷ 64", P5_CYAN),
                ],
            );
            spawn_math_formula(
                side,
                fonts,
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

pub fn spawn_aba_problem_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    let code_lines = vec![
        vec![(
            "// Monotonic sequence stamps (Crossbeam / Vyukov turn scheme):",
            TokenKind::Comment,
        )],
        vec![
            ("pub fn ", TokenKind::Keyword),
            ("read", TokenKind::Function),
            ("(&self) -> ", TokenKind::Plain),
            ("Result", TokenKind::Type),
            ("<T, ()> {", TokenKind::Plain),
        ],
        vec![("    loop {", TokenKind::Plain)],
        vec![
            ("        let idx = self.read_idx.", TokenKind::Plain),
            ("load", TokenKind::Function),
            ("(Ordering::Acquire);", TokenKind::Plain),
        ],
        vec![
            ("        let widx = self.write_idx.", TokenKind::Plain),
            ("load", TokenKind::Function),
            ("(Ordering::Acquire);", TokenKind::Plain),
        ],
        vec![("        let i = idx % self.capacity;", TokenKind::Plain)],
        vec![(
            "        if self.empty(idx, widx) { return Err(()); }",
            TokenKind::Plain,
        )],
        vec![
            ("        if self.buffer[i].stamp.", TokenKind::Plain),
            ("load", TokenKind::Function),
            ("(Ordering::Acquire) != idx {", TokenKind::Plain),
        ],
        vec![("            spin_loop(); continue;", TokenKind::Plain)],
        vec![("        }", TokenKind::Plain)],
        vec![(
            "        if self.read_idx.compare_exchange_weak(",
            TokenKind::Plain,
        )],
        vec![(
            "            idx, idx.wrapping_add(1), AcqRel, Relaxed",
            TokenKind::Plain,
        )],
        vec![(
            "        ).is_err() { spin_loop(); continue; }",
            TokenKind::Plain,
        )],
        vec![(
            "        let rr = unsafe { self.buffer[i].data.get().read().assume_init() };",
            TokenKind::Plain,
        )],
        vec![(
            "        // Offset uninit stamp for next lap (Crossbeam scheme):",
            TokenKind::Comment,
        )],
        vec![(
            "        self.buffer[i].stamp.store(idx.wrapping_add(self.capacity + 1), Release);",
            TokenKind::Plain,
        )],
        vec![("        return Ok(rr);", TokenKind::Plain)],
        vec![("    }", TokenKind::Plain)],
        vec![("}", TokenKind::Plain)],
    ];

    spawn_slide_scaffold(
        &mut commands,
        &font_assets,
        SlideState::AbaProblem,
        "/// EXHIBIT F // RETHINKING STATES ///",
        "THE ABA PROBLEM RESOLUTION",
        "stamp_ring_buffer.rs",
        "RUST // TURN-STAMP",
        code_lines,
        |side, fonts| {
            spawn_ring_buffer_diagram(
                side,
                fonts.monospace.clone(),
                "MONOTONIC STAMP RING BUFFER (N=4)",
                vec![
                    SlotState {
                        label: "Slot 0",
                        value: "0xAA",
                        state: "stamp: 0",
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
                        state: "stamp: 4",
                        is_highlighted: false,
                        is_error: false,
                    },
                ],
                0,
                3,
            );
            spawn_math_formula(
                side,
                fonts,
                "/// STAMP TRANSITION INVARIANT ///",
                &[
                    ("Read: ", P5_MUTED),
                    ("stamp = idx", P5_WHITE),
                    (" | Uninit: ", P5_MUTED),
                    ("idx + cap + 1", P5_CYAN),
                ],
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "★",
                    title: "ABA IMMUNITY",
                    text: "Monotonically advancing read_idx and write_idx with per-slot stamps (idx on write, idx + cap + 1 on read) distinguishes every lap around the ring.",
                    is_alert: false,
                },
            );
        },
    );
}

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
            "TRACE loom::rt: yield_now thread=Id(2) switch=true",
            TokenKind::Plain,
        )],
        vec![(
            "// 3-Thread Test: Model exceeded maximum number of branches (19! * 3)",
            TokenKind::Alert,
        )],
        vec![(
            "// 2-Thread Test: Causality violation: Concurrent accesses to UnsafeCell!",
            TokenKind::Alert,
        )],
        vec![("", TokenKind::Plain)],
        vec![(
            "// Part 2 Fix: 3 targeted 2-thread Loom tests (cuts state explosion):",
            TokenKind::Comment,
        )],
        vec![("// 1. Read / Write pair starting EMPTY", TokenKind::Plain)],
        vec![("// 2. Write / Write pair starting EMPTY", TokenKind::Plain)],
        vec![("// 3. Read / Read pair starting FULL", TokenKind::Plain)],
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
                fonts,
                "/// SPIN LOCK BRANCH EXPLOSION ///",
                &[
                    ("Iter 68,005: ", P5_MUTED),
                    ("Max Branches Exceeded (19! × 3)", P5_RED),
                ],
            );
            spawn_math_formula(
                side,
                fonts,
                "/// BOUNDARY RACE DISCOVERY ///",
                &[
                    ("Loom Detected: ", P5_MUTED),
                    ("Causality Violation (UnsafeCell)", P5_BRIGHT_RED),
                ],
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "!",
                    title: "TARGETED 2-THREAD BOUNDARY TESTS",
                    text: "Loom caught a concurrent UnsafeCell boundary race that random loops missed. Splitting 3-thread tests into R/W (empty), W/W (empty), and R/R (full) pairs tames state explosion.",
                    is_alert: true,
                },
            );
        },
    );
}

pub fn spawn_recovery_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    let code_lines = vec![
        vec![(
            "// Critical section between index CAS and stamp commit:",
            TokenKind::Comment,
        )],
        vec![
            ("if self.write_idx.", TokenKind::Plain),
            ("compare_exchange_weak", TokenKind::Function),
            (
                "(idx, idx.wrapping_add(1), ...).is_err() {",
                TokenKind::Plain,
            ),
        ],
        vec![("    continue;", TokenKind::Plain)],
        vec![("}", TokenKind::Plain)],
        vec![("", TokenKind::Plain)],
        vec![(
            "// Step 2: Raw bit copy from stack (panic-free in Rust):",
            TokenKind::Comment,
        )],
        vec![
            ("unsafe { core::ptr::", TokenKind::Plain),
            ("write", TokenKind::Function),
            (
                "(self.buffer[i].data.get(), MaybeUninit::new(v)) };",
                TokenKind::Plain,
            ),
        ],
        vec![(
            "// Step 3: Atomic stamp store (panic-free in Rust):",
            TokenKind::Comment,
        )],
        vec![
            ("self.buffer[i].stamp.", TokenKind::Plain),
            ("store", TokenKind::Function),
            ("(idx, Ordering::Release);", TokenKind::Plain),
        ],
        vec![("", TokenKind::Plain)],
        vec![(
            "// If OS forcefully kills thread (SIGKILL) before Step 3:",
            TokenKind::Alert,
        )],
        vec![(
            "// Slot 2 { stamp: 0, val: 0x0 } -> Readers spin on uninit, Writers see full!",
            TokenKind::Alert,
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
                &["1. CAS Index", "2. ptr::write", "3. Store Stamp"],
                Some(1),
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "!",
                    title: "LOCKLESS != LOCK-FREE",
                    text: "Vyukov's 2010 MPMC design is lockless (per-slot blocking), trading pointer-CAS and epoch reclamation overhead for cache locality.",
                    is_alert: true,
                },
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "◆",
                    title: "PANIC-FREE CRITICAL SECTION",
                    text: "Because raw bit copies and atomic stores cannot panic, only an external OS SIGKILL during those two instructions can orphan a slot.",
                    is_alert: false,
                },
            );
        },
    );
}

pub fn spawn_automate_recovery_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    let code_lines = vec![
        vec![(
            "// Attempting peer-thread recovery with local retry counter:",
            TokenKind::Comment,
        )],
        vec![("let mut retry = (0, idx);", TokenKind::Plain)],
        vec![("loop {", TokenKind::Plain)],
        vec![
            ("    if self.buffer[i].stamp.", TokenKind::Plain),
            ("load", TokenKind::Function),
            ("(Ordering::Acquire) != ridx {", TokenKind::Plain),
        ],
        vec![("        spin_loop();", TokenKind::Plain)],
        vec![("        match retry {", TokenKind::Plain)],
        vec![
            (
                "            (cnt, id) if cnt >= 10 && id == idx => self.",
                TokenKind::Plain,
            ),
            ("fix", TokenKind::Function),
            ("(i), // ABA returns!", TokenKind::Alert),
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
        vec![("", TokenKind::Plain)],
        vec![
            ("fn ", TokenKind::Keyword),
            ("fix", TokenKind::Function),
            ("(&self, i: usize) {", TokenKind::Plain),
        ],
        vec![(
            "    let read = self.read_idx.load(Ordering::Acquire);",
            TokenKind::Plain,
        )],
        vec![(
            "    if self.buffer[(read + 1) % self.capacity].stamp.load(Acquire) == read + 1 {",
            TokenKind::Plain,
        )],
        vec![(
            "        self.buffer[i].stamp.store(...); // Skip orphaned slot",
            TokenKind::Plain,
        )],
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
                fonts,
                "/// EXACT FORMER-STATE CRASH CHECKS ///",
                &[
                    ("Write Crash: ", P5_MUTED),
                    ("stamp = idx − cap + 1", P5_RED),
                ],
            );
            spawn_math_formula(
                side,
                fonts,
                "/// ADJACENT SLOT HEURISTIC ///",
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
                    text: "Even exact former-state checks (stamp == idx - cap + 1) cannot distinguish a killed thread from an OS-paused thread. Auto-fixing reintroduces ABA; expose manual fix() to clients instead.",
                    is_alert: true,
                },
            );
        },
    );
}

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
                fonts,
                "/// L1 MISS RATE FORMULA ///",
                &[
                    ("239M Misses ", P5_RED),
                    ("÷ ", P5_MUTED),
                    ("471M Loads ", P5_WHITE),
                    ("= ", P5_MUTED),
                    ("50.74%", P5_BRIGHT_RED),
                ],
            );
            spawn_math_formula(
                side,
                fonts,
                "/// CAS FAILURE RATE FORMULA ///",
                &[
                    ("50M Fails ", P5_RED),
                    ("÷ ", P5_MUTED),
                    ("74M Total ", P5_WHITE),
                    ("= ", P5_MUTED),
                    ("67.17% (⅔ Wasted!)", P5_BRIGHT_RED),
                ],
            );
        },
    );
}

pub fn spawn_disassembled_code_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    let code_lines = vec![
        vec![(
            "// Disassembly revealed two major bottlenecks:",
            TokenKind::Comment,
        )],
        vec![(
            "// 1. read_idx (+0x00) & write_idx (+0x08) share a 128B L1 cache line",
            TokenKind::Comment,
        )],
        vec![(
            "// 2. idx % capacity compiles to udiv + msub (2 to 34 CPU cycles!)",
            TokenKind::Comment,
        )],
        vec![("#[repr(align(128))]", TokenKind::Keyword)],
        vec![
            ("pub struct ", TokenKind::Keyword),
            ("CachePadded", TokenKind::Type),
            ("<T>(pub T);", TokenKind::Plain),
        ],
        vec![("", TokenKind::Plain)],
        vec![
            ("pub struct ", TokenKind::Keyword),
            ("RingBuffer", TokenKind::Type),
            ("<T> {", TokenKind::Plain),
        ],
        vec![(
            "    read_idx: CachePadded<AtomicUsize>,  // Isolated 128B line",
            TokenKind::Plain,
        )],
        vec![(
            "    write_idx: CachePadded<AtomicUsize>, // +128B offset (#128 in ASM)",
            TokenKind::Plain,
        )],
        vec![("}", TokenKind::Plain)],
        vec![("", TokenKind::Plain)],
        vec![(
            "// Criterion + Instruments after 128B CachePadded:",
            TokenKind::Comment,
        )],
        vec![(
            "// Time: 18.30 ms -> 12.86 ms (-30.6% wall-clock reduction!)",
            TokenKind::Keyword,
        )],
        vec![(
            "// L1 Misses: 50.74% -> 31.22% | IPC: 0.0306 -> 0.0450",
            TokenKind::Keyword,
        )],
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
                fonts,
                "/// 128B CACHE PADDED SPEEDUP ///",
                &[
                    ("18.30 ms ", P5_RED),
                    ("──▶ ", P5_MUTED),
                    ("12.86 ms (−30.6%)", P5_GOLD),
                ],
            );
            spawn_math_formula(
                side,
                fonts,
                "/// L1 MISS & IPC IMPROVEMENT ///",
                &[
                    ("L1 Miss: ", P5_MUTED),
                    ("31.22%", P5_CYAN),
                    (" | IPC: ", P5_MUTED),
                    ("0.0450", P5_WHITE),
                ],
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "!",
                    title: "FALSE SHARING & UDIV DISCOVERY",
                    text: "Padding indices to 128-byte Apple Silicon L1 lines cut runtime by 30.6% and L1 misses to 31.2%. Disassembly also exposed udiv + msub modulo instructions as the next bottleneck.",
                    is_alert: true,
                },
            );
        },
    );
}

pub fn spawn_branchless_index_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    let code_lines = vec![
        vec![(
            "// Ken's branchless index computation (Paving Lock Freedom Part 2):",
            TokenKind::Comment,
        )],
        vec![
            ("let idx = self.read_idx.", TokenKind::Plain),
            ("load", TokenKind::Function),
            ("(Ordering::Acquire);", TokenKind::Plain),
        ],
        vec![
            ("let mcb = (self.capacity + 1).", TokenKind::Plain),
            ("next_power_of_two", TokenKind::Function),
            ("();", TokenKind::Plain),
        ],
        vec![
            ("let i = idx & (mcb - 1); ", TokenKind::Plain),
            ("// Mask lower bits for counter", TokenKind::Comment),
        ],
        vec![("", TokenKind::Plain)],
        vec![(
            "// Save condition to 0 or 1 without branching:",
            TokenKind::Comment,
        )],
        vec![
            (
                "let at_capacity = (i + 1 >= self.capacity) as ",
                TokenKind::Plain,
            ),
            ("usize", TokenKind::Type),
            (";", TokenKind::Plain),
        ],
        vec![("", TokenKind::Plain)],
        vec![(
            "// at_capacity=0 => 0.wrapping_sub(1) = usize::MAX, passes idx + 1",
            TokenKind::Comment,
        )],
        vec![(
            "// at_capacity=1 => 1.wrapping_sub(1) = 0, zeroes idx & flips lap bit:",
            TokenKind::Comment,
        )],
        vec![
            ("let new_idx = ((idx + 1) & at_capacity.", TokenKind::Plain),
            ("wrapping_sub", TokenKind::Function),
            ("(1))", TokenKind::Plain),
        ],
        vec![(
            "    | (at_capacity * ((idx & mcb) ^ mcb));",
            TokenKind::Plain,
        )],
    ];

    spawn_slide_scaffold(
        &mut commands,
        &font_assets,
        SlideState::BranchlessIndex,
        "/// EXHIBIT L // INSTRUCTION TUNING ///",
        "BRANCHLESS COMPUTATION",
        "branchless_index.rs",
        "RUST // ZERO BRANCHES",
        code_lines,
        |side, fonts| {
            spawn_math_formula(
                side,
                fonts,
                "/// BRANCHLESS MASK: wrapping_sub(1) ///",
                &[
                    ("at_cap=0: 0 - 1 = ", P5_MUTED),
                    ("usize::MAX", P5_CYAN),
                    (" ⟹ idx + 1", P5_WHITE),
                ],
            );
            spawn_math_formula(
                side,
                fonts,
                "/// LAP BIT FLIP: at_capacity=1 ///",
                &[
                    ("1 - 1 = 0 ⟹ ", P5_MUTED),
                    ("0 | ((idx & mcb) ^ mcb)", P5_GOLD),
                ],
            );
            spawn_math_formula(
                side,
                fonts,
                "/// UNCONDITIONAL EVALUATION ///",
                &[
                    ("Identical ALU Op: ", P5_MUTED),
                    ("1 Cycle (No Jump)", P5_WHITE),
                ],
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "⚡",
                    title: "PURE ARITHMETIC WRAPPING",
                    text: "Replaces `if i + 1 >= capacity` with wrapping arithmetic bitmasks. The CPU pipeline executes identical instructions whether wrapping or incrementing, eliminating branch mispredictions.",
                    is_alert: false,
                },
            );
        },
    );
}

pub fn spawn_tail_latency_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    let code_lines = vec![
        vec![(
            "// 1. Define Rust USDT probes (compiles to NOPs when inactive):",
            TokenKind::Comment,
        )],
        vec![("#[usdt::provider]", TokenKind::Keyword)],
        vec![
            ("mod ", TokenKind::Keyword),
            ("limitless_probes {", TokenKind::Plain),
        ],
        vec![(
            "    fn read__start(_: &usdt::UniqueId) {}",
            TokenKind::Plain,
        )],
        vec![(
            "    fn read__done(_: &usdt::UniqueId, idx: u64, ok: u8) {}",
            TokenKind::Plain,
        )],
        vec![("}", TokenKind::Plain)],
        vec![("let probe_id = usdt::UniqueId::new();", TokenKind::Plain)],
        vec![(
            "limitless_probes::read__start!(|| &probe_id);",
            TokenKind::Plain,
        )],
        vec![(
            "limitless_probes::read__done!(|| (&probe_id, i as u64, 1u8));",
            TokenKind::Plain,
        )],
        vec![("", TokenKind::Plain)],
        vec![(
            "// 2. DTrace script (tail_latency.d) bucketing by power of two:",
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
                fonts,
                "/// USDT COMPILER OVERHEAD ///",
                &[
                    ("Production Disabled: ", P5_MUTED),
                    ("1 NOP (0.0 ns)", P5_CYAN),
                ],
            );
            spawn_math_formula(
                side,
                fonts,
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
            "  128 |                      25    256 |@@@@@@             2950",
            TokenKind::Plain,
        )],
        vec![(
            "  256 |@@@@@@@@@@@@@@@@@@ 17932    512 |@@@@@@@@@@@@@@@@   8316",
            TokenKind::Plain,
        )],
        vec![(
            "  512 |@@@@@               4943   1024 |@@@@@              2510",
            TokenKind::Plain,
        )],
        vec![(
            " 1024 |@@                  1818  32768 |                      2",
            TokenKind::Plain,
        )],
        vec![(
            "16384 |                       3 131072 |                      1",
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
                "DTRACE READ LATENCY HISTOGRAM",
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
                fonts,
                "/// READ & WRITE TAIL SPIKES ///",
                &[
                    ("Read: ", P5_MUTED),
                    ("64× (16.4 µs)", P5_RED),
                    (" | Write: ", P5_MUTED),
                    ("256× (131 µs)", P5_BRIGHT_RED),
                ],
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "!",
                    title: "WHY TAIL LATENCY IS UNBOUNDED",
                    text: "Measured across 4 read and 4 write threads. Because the algorithm is lockless rather than lock-free, an OS-preempted thread holding a slot forces peers to spin-wait indefinitely.",
                    is_alert: true,
                },
            );
        },
    );
}

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
                fonts,
                "/// UNCONTROLLED TAIL SPIKE ///",
                &[
                    ("Tight CAS Spin: ", P5_MUTED),
                    ("131,072 ns (256×)", P5_RED),
                ],
            );
            spawn_math_formula(
                side,
                fonts,
                "/// BUS STORM RELIEF ///",
                &[
                    ("With Backoff: ", P5_MUTED),
                    ("Matches Crossbeam Queue", P5_WHITE),
                ],
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "★",
                    title: "BUS STORM RELIEF",
                    text: "Spinning aggressively saturates the memory bus. Backoff throttles atomic retry frequency and matches Crossbeam's benchmark performance, though preemption tail latency remains unbounded.",
                    is_alert: false,
                },
            );
        },
    );
}

pub fn spawn_scaling_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    let code_lines = vec![
        vec![(
            "[Core 1] [Core 2] [Core 3] [Core 4] ---> Contending on read_idx",
            TokenKind::Plain,
        )],
        vec![("  |         |        |        |", TokenKind::Comment)],
        vec![(
            " [==== CPU INTERCONNECT MESI INVALIDATION SATURATION ====]",
            TokenKind::Keyword,
        )],
        vec![("  |         |        |        |", TokenKind::Comment)],
        vec![(
            "[Core 5] [Core 6] [Core 7] [Core 8] ---> Contending on write_idx",
            TokenKind::Plain,
        )],
        vec![("", TokenKind::Plain)],
        vec![(
            "// Optimization Journey (Criterion & Instruments on Apple Silicon):",
            TokenKind::Comment,
        )],
        vec![(
            "// 1. Unpadded + Modulo:   18.30 ms | 50.74% L1 miss | 0.0306 IPC",
            TokenKind::Plain,
        )],
        vec![(
            "// 2. 128B CachePadded:    12.86 ms | 31.22% L1 miss | 0.0450 IPC",
            TokenKind::Plain,
        )],
        vec![(
            "// 3. Padded Slot<T>:      12.60 ms | 25.62% L1 miss | 3456K RAM",
            TokenKind::Plain,
        )],
        vec![(
            "// 4. MCB Bit-Walk:         2.74 ms | 17.19% L1 miss | 0.0838 IPC",
            TokenKind::Keyword,
        )],
        vec![(
            "// 5. Crossbeam / Backoff:  5.26 ms | Prevents multi-core bus storms",
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
                "CRITERION RUNTIME (LOWER IS FASTER)",
                200.0,
                vec![
                    BarEntry {
                        label: "1. Unpadded + udiv",
                        value_text: "18.30 ms (50.7% miss)",
                        fraction: 1.00,
                        color: P5_BRIGHT_RED,
                    },
                    BarEntry {
                        label: "2. 128B CachePadded",
                        value_text: "12.86 ms (31.2% miss)",
                        fraction: 0.70,
                        color: P5_RED,
                    },
                    BarEntry {
                        label: "3. Padded Slot<T>",
                        value_text: "12.60 ms (25.6% miss)",
                        fraction: 0.69,
                        color: P5_MUTED,
                    },
                    BarEntry {
                        label: "4. MCB Bit-Walk",
                        value_text: "2.74 ms (17.2% miss)",
                        fraction: 0.15,
                        color: P5_GOLD,
                    },
                    BarEntry {
                        label: "5. Crossbeam Queue",
                        value_text: "5.26 ms (With Backoff)",
                        fraction: 0.29,
                        color: P5_CYAN,
                    },
                ],
            );
            spawn_callout_card(
                side,
                fonts,
                CalloutCard {
                    icon: "!",
                    title: "SHARED INDEX CONTENTION WALL",
                    text: "Even after a 6.7x speedup (18.3 ms -> 2.74 ms), MPMC scales only so far: every reader and writer still hammers shared atomic indices across the core interconnect.",
                    is_alert: true,
                },
            );
        },
    );
}

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
                fonts,
                "/// SPSC SYNCHRONIZATION OVERHEAD ///",
                &[
                    ("Atomic CAS Instructions: ", P5_MUTED),
                    ("0 (Zero Contention)", P5_WHITE),
                ],
            );
            spawn_math_formula(
                side,
                fonts,
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
