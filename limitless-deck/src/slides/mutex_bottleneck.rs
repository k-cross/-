use bevy::prelude::*;
use bevy::state::state_scoped::DespawnOnExit;
use crate::theme::colors::*;
use crate::theme::geometry::*;
use crate::theme::typography::*;
use crate::slideshow::SlideState;
use crate::slideshow::code_view::{spawn_styled_code_block, TokenKind};
use crate::slideshow::animation::{SlamEntrance, PunkJitter};

pub fn spawn_mutex_bottleneck_slide(mut commands: Commands) {
    // Root full-screen slide container
    commands.spawn((
        Node {
            position_type: PositionType::Absolute,
            width: Val::Percent(100.0),
            height: Val::Percent(100.0),
            padding: UiRect {
                left: Val::Px(70.0),
                right: Val::Px(70.0),
                top: Val::Px(80.0),
                bottom: Val::Px(70.0),
            },
            flex_direction: FlexDirection::Column,
            justify_content: JustifyContent::SpaceBetween,
            align_items: AlignItems::FlexStart,
            ..default()
        },
        Transform::default(),
        Visibility::default(),
        InheritedVisibility::default(),
        DespawnOnExit(SlideState::MutexBottleneck),
    )).with_children(|slide| {
        // --- Header Section ---
        slide.spawn((
            Node {
                flex_direction: FlexDirection::Column,
                align_items: AlignItems::FlexStart,
                row_gap: Val::Px(10.0),
                ..default()
            },
            Transform::default(),
            Visibility::default(),
            InheritedVisibility::default(),
            SlamEntrance::new(Vec2::new(-240.0, 100.0), -0.2, 0.0),
        )).with_children(|header| {
            // Category Ribbon with Prison Barcode Tag
            header.spawn((
                Node {
                    padding: UiRect::axes(Val::Px(14.0), Val::Px(6.0)),
                    border: UiRect::all(Val::Px(2.0)),
                    ..default()
                },
                BackgroundColor(P5_RED),
                BorderColor::all(P5_WHITE),
                rot_counter(),
                PunkJitter::new(-0.038, 0.015),
            )).with_children(|tag| {
                tag.spawn((
                    Text::new("/// EXHIBIT A // MUTEX VS ATOMIC PRIMITIVES ///"),
                    TextFont::from_font_size(FONT_BODY_SM),
                    TextColor(P5_WHITE),
                ));
            });

            // Title with Crimson Drop-Shadow Cutout
            header.spawn((
                Node {
                    padding: UiRect::axes(Val::Px(20.0), Val::Px(8.0)),
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
            )).with_children(|t| {
                t.spawn((
                    Text::new("THE COST OF SERIALIZATION"),
                    TextFont::from_font_size(FONT_HEADING_XL),
                    TextColor(P5_WHITE),
                ));
            });
        });

        // --- Main Comparison Section: Code Block & Tactical Analysis ---
        slide.spawn((
            Node {
                width: Val::Percent(100.0),
                flex_direction: FlexDirection::Row,
                justify_content: JustifyContent::SpaceBetween,
                align_items: AlignItems::Center,
                column_gap: Val::Px(36.0),
                ..default()
            },
            Transform::default(),
            Visibility::default(),
            InheritedVisibility::default(),
        )).with_children(|row| {
            // Left: Clean, clear code layout inside P5 terminal frame (Slams in from left)
            let code_lines = vec![
                vec![
                    ("// Locked Mutex: Heavy OS kernel suspension", TokenKind::Comment),
                ],
                vec![
                    ("pub fn ", TokenKind::Keyword),
                    ("increment_mutex", TokenKind::Function),
                    ("(counter: &", TokenKind::Plain),
                    ("Mutex", TokenKind::Type),
                    ("<", TokenKind::Plain),
                    ("u64", TokenKind::Type),
                    (">) {", TokenKind::Plain),
                ],
                vec![
                    ("    let mut ", TokenKind::Keyword),
                    ("guard = counter.", TokenKind::Plain),
                    ("lock", TokenKind::Function),
                    ("().", TokenKind::Plain),
                    ("unwrap", TokenKind::Function),
                    ("();", TokenKind::Plain),
                ],
                vec![
                    ("    *guard += ", TokenKind::Plain),
                    ("1", TokenKind::Number),
                    (";", TokenKind::Plain),
                ],
                vec![
                    ("}", TokenKind::Plain),
                ],
                vec![
                    ("", TokenKind::Plain),
                ],
                vec![
                    ("// Lock-Free: Hardware atomic instruction (single CPU cycle)", TokenKind::Comment),
                ],
                vec![
                    ("pub fn ", TokenKind::Keyword),
                    ("increment_atomic", TokenKind::Function),
                    ("(counter: &", TokenKind::Plain),
                    ("AtomicU64", TokenKind::Type),
                    (") {", TokenKind::Plain),
                ],
                vec![
                    ("    counter.", TokenKind::Plain),
                    ("fetch_add", TokenKind::Function),
                    ("(", TokenKind::Plain),
                    ("1", TokenKind::Number),
                    (", ", TokenKind::Plain),
                    ("Ordering", TokenKind::Type),
                    ("::", TokenKind::Plain),
                    ("Relaxed", TokenKind::Type),
                    (");", TokenKind::Plain),
                ],
                vec![
                    ("}", TokenKind::Plain),
                ],
            ];

            // Code container wrapper with SlamEntrance
            row.spawn((
                Node {
                    flex_direction: FlexDirection::Column,
                    ..default()
                },
                SlamEntrance::new(Vec2::new(-300.0, 40.0), -0.15, 0.08),
            )).with_children(|code_wrapper| {
                spawn_styled_code_block(
                    code_wrapper,
                    "concurrency_primitives.rs",
                    "RUST // ATOMICS",
                    code_lines,
                );
            });

            // Right: Tactical Persona 5 Callout Cards (Slams in from right)
            row.spawn((
                Node {
                    flex_direction: FlexDirection::Column,
                    row_gap: Val::Px(16.0),
                    width: Val::Px(360.0),
                    ..default()
                },
                rot_counter(),
                SlamEntrance::new(Vec2::new(320.0, 60.0), 0.2, 0.14),
            )).with_children(|side| {
                // Card 1: Mutex Penalty (Captivity / Stalled Threads)
                side.spawn((
                    Node {
                        flex_direction: FlexDirection::Column,
                        padding: UiRect::all(Val::Px(16.0)),
                        row_gap: Val::Px(6.0),
                        border: UiRect {
                            left: Val::Px(5.0),
                            top: Val::Px(2.0),
                            right: Val::Px(2.0),
                            bottom: Val::Px(2.0),
                        },
                        ..default()
                    },
                    BackgroundColor(P5_BLACK),
                    BorderColor {
                        left: P5_RED,
                        top: P5_WHITE,
                        right: P5_WHITE,
                        bottom: P5_WHITE,
                    },
                    PunkJitter::new(0.0, 0.012),
                )).with_children(|c1| {
                    c1.spawn(Node {
                        flex_direction: FlexDirection::Row,
                        align_items: AlignItems::Center,
                        column_gap: Val::Px(6.0),
                        ..default()
                    }).with_children(|h| {
                        h.spawn((
                            Text::new("⛓"),
                            TextFont::from_font_size(FONT_BODY_ICON),
                            TextColor(P5_RED),
                        ));
                        h.spawn((
                            Text::new("MUTEX PENALTY (CAPTIVITY)"),
                            TextFont::from_font_size(FONT_BODY_LG),
                            TextColor(P5_RED),
                        ));
                    });
                    c1.spawn((
                        Text::new("Contended locks trigger syscalls, context switches, and cache thrashing. Threads are jailed in OS sleep queues."),
                        TextFont::from_font_size(FONT_CAPTION),
                        TextColor(P5_OFF_WHITE),
                    ));
                });

                // Card 2: Lock-Free Advantage (Liberation / Bare Metal)
                side.spawn((
                    Node {
                        flex_direction: FlexDirection::Column,
                        padding: UiRect::all(Val::Px(16.0)),
                        row_gap: Val::Px(6.0),
                        border: UiRect {
                            left: Val::Px(2.0),
                            top: Val::Px(2.0),
                            right: Val::Px(5.0),
                            bottom: Val::Px(2.0),
                        },
                        ..default()
                    },
                    BackgroundColor(P5_BLACK),
                    BorderColor {
                        left: P5_WHITE,
                        top: P5_WHITE,
                        right: P5_RED,
                        bottom: P5_WHITE,
                    },
                    PunkJitter::new(0.0, 0.012),
                )).with_children(|c2| {
                    c2.spawn(Node {
                        flex_direction: FlexDirection::Row,
                        align_items: AlignItems::Center,
                        column_gap: Val::Px(6.0),
                        ..default()
                    }).with_children(|h| {
                        h.spawn((
                            Text::new("★"),
                            TextFont::from_font_size(FONT_BODY_ICON),
                            TextColor(P5_WHITE),
                        ));
                        h.spawn((
                            Text::new("LOCK-FREE ADVANTAGE (FREEDOM)"),
                            TextFont::from_font_size(FONT_BODY_LG),
                            TextColor(P5_WHITE),
                        ));
                    });
                    c2.spawn((
                        Text::new("Executes via CPU hardware cache coherency bus protocol (MESI). Never blocks or puts the thread to sleep."),
                        TextFont::from_font_size(FONT_CAPTION),
                        TextColor(P5_OFF_WHITE),
                    ));
                });
            });
        });

        // Bottom spacer to ensure room for HUD
        slide.spawn(Node {
            height: Val::Px(20.0),
            ..default()
        });
    });
}
