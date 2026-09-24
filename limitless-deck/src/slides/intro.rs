use bevy::prelude::*;
use bevy::state::state_scoped::DespawnOnExit;
use crate::theme::colors::*;
use crate::theme::geometry::*;
use crate::slideshow::SlideState;
use crate::slideshow::FontAssets;
use crate::slideshow::animation::{SlamEntrance, PunkJitter, BobbingCursor};
use crate::slideshow::splatter::{spawn_ink_blotch, spawn_ui_ink_blotch, spawn_corner_ink_splatter, CardCorner};
use crate::theme::cutout;
use crate::theme::starburst;
use crate::theme::typography::*;

#[allow(dead_code)]
pub struct CutoutLetter {
    pub ch: &'static str,
    pub font: Option<Handle<Font>>,
    pub font_size: f32,
    pub bg: Color,
    pub fg: Color,
    pub border: UiRect,
    pub border_color: BorderColor,
    pub border_radius: BorderRadius,
    pub offset_y: f32,
    pub tilt: f32,
    pub pad_h: f32,
    pub pad_v: f32,
    pub slam_offset: Vec2,
    pub slam_rot: f32,
    pub delay: f32,
}

#[allow(dead_code)]
pub fn spawn_cutout_letter(builder: &mut ChildSpawnerCommands, letter: CutoutLetter) {
    let mut text_font = TextFont::from_font_size(letter.font_size);
    if let Some(font) = letter.font {
        text_font = text_font.with_font(font);
    }
    builder.spawn((
        Node {
            padding: UiRect::axes(Val::Px(letter.pad_h), Val::Px(letter.pad_v)),
            border: letter.border,
            border_radius: letter.border_radius,
            margin: UiRect {
                left: Val::Px(cutout::SCRAP_MARGIN),
                right: Val::Px(cutout::SCRAP_MARGIN),
                top: Val::Px(letter.offset_y.max(0.0)),
                bottom: Val::Px((-letter.offset_y).max(0.0)),
            },
            ..default()
        },
        BackgroundColor(letter.bg),
        letter.border_color,
        Transform::from_rotation(Quat::from_rotation_z(letter.tilt)),
        Visibility::default(),
        InheritedVisibility::default(),
        SlamEntrance::new(letter.slam_offset, letter.slam_rot, letter.delay),
        PunkJitter::new(letter.tilt, 0.055),
    )).with_children(|scrap| {
        scrap.spawn((
            Text::new(letter.ch),
            text_font,
            TextColor(letter.fg),
        ));
    });
}

pub fn spawn_intro_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    // Slide-scoped high-visibility crimson ink blotch in world space
    let title_blotch = spawn_ink_blotch(
        &mut commands,
        Vec2::new(-280.0, 95.0),
        -65.0,
        P5_RED,
        85.0,
        Vec2::new(1.1, -0.5),
    );
    commands.entity(title_blotch).insert(DespawnOnExit(SlideState::Intro));

    // Slide-scoped high-voltage crimson ink spray across the bottom divider
    let crimson_spray = spawn_ink_blotch(
        &mut commands,
        Vec2::new(-160.0, -115.0),
        -64.0,
        P5_BRIGHT_RED,
        42.0,
        Vec2::new(1.4, 0.4),
    );
    commands.entity(crimson_spray).insert(DespawnOnExit(SlideState::Intro));

    // Root full-screen slide container for intro
    commands.spawn((
        Node {
            position_type: PositionType::Absolute,
            width: Val::Percent(100.0),
            height: Val::Percent(100.0),
            padding: UiRect {
                left: Val::Px(56.0),
                right: Val::Px(56.0),
                top: Val::Px(72.0),
                bottom: Val::Px(64.0),
            },
            flex_direction: FlexDirection::Row,
            justify_content: JustifyContent::SpaceBetween,
            align_items: AlignItems::Center,
            ..default()
        },
        Transform::default(),
        Visibility::default(),
        InheritedVisibility::default(),
        DespawnOnExit(SlideState::Intro),
    )).with_children(|slide| {
        // ==============================================================
        // PROCEDURAL ORGANIC UI INK SPLATTERS (High-contrast, fluid circular puddles)
        // ==============================================================
        // 1. High-voltage crimson ink pool bleeding from behind the ransom title
        spawn_ui_ink_blotch(slide, Vec2::new(260.0, 160.0), 48.0, P5_RED, Vec2::new(1.3, -0.6));

        // 2. Secondary stark crimson spray near the "HOLD UP!" starburst
        spawn_ui_ink_blotch(slide, Vec2::new(210.0, 75.0), 24.0, P5_BRIGHT_RED, Vec2::new(-0.9, -0.8));

        // 3. Dark crimson ink stain bleeding under the zine menu divider
        spawn_ui_ink_blotch(slide, Vec2::new(320.0, 310.0), 36.0, P5_DARK_RED, Vec2::new(1.2, 0.5));

        // 4. Crimson spray splatter bleeding past the Phantom Calling Card seal
        spawn_ui_ink_blotch(slide, Vec2::new(1100.0, 480.0), 32.0, P5_RED, Vec2::new(0.9, 0.7));

        // ==============================================================
        // LEFT COLUMN: Anarchic Punk Zine Magazine-Cutout Title & Agenda
        // ==============================================================
        slide.spawn((
            Node {
                flex_direction: FlexDirection::Column,
                align_items: AlignItems::FlexStart,
                row_gap: Val::Px(12.0),
                max_width: Val::Px(700.0),
                ..default()
            },
            Transform::default(),
            Visibility::default(),
            InheritedVisibility::default(),
        )).with_children(|col| {
            // ==============================================================
            // Top Row: JAGGED COMIC STARBURST "HOLD UP!" + Classification Tag
            // ==============================================================
            col.spawn((
                Node {
                    flex_direction: FlexDirection::Row,
                    align_items: AlignItems::Center,
                    column_gap: Val::Px(12.0),
                    ..default()
                },
                Transform::default(),
                Visibility::default(),
                InheritedVisibility::default(),
            )).with_children(|top_action| {
                // "HOLD UP!" Multi-Point Comic Action Starburst
                top_action.spawn((
                    Node {
                        position_type: PositionType::Relative,
                        padding: UiRect::axes(Val::Px(starburst::STARBURST_PAD.0), Val::Px(starburst::STARBURST_PAD.1)),
                        border: starburst::starburst_border(),
                        ..default()
                    },
                    BackgroundColor(P5_WHITE),
                    BorderColor {
                        left: P5_RED,
                        top: P5_BLACK,
                        right: P5_RED,
                        bottom: P5_RED,
                    },
                    Transform::from_rotation(Quat::from_rotation_z(starburst::STARBURST_TILT)), // +5.7 deg
                    Visibility::default(),
                    InheritedVisibility::default(),
                    SlamEntrance::new(Vec2::new(-250.0, 120.0), -0.3, 0.0),
                    PunkJitter::new(starburst::STARBURST_TILT, 0.015),
                )).with_children(|hold_up| {
                    // Jagged explosive comic spikes behind text
                    hold_up.spawn((
                        Text::new("★"),
                        TextFont::from_font_size(14.0).with_font(font_assets.symbols.clone()),
                        TextColor(P5_RED),
                        Node {
                            margin: UiRect::right(Val::Px(4.0)),
                            ..default()
                        },
                    ));
                    hold_up.spawn((
                        Text::new("HOLD UP!"),
                        TextFont::from_font_size(FONT_BODY_ICON).with_font(font_assets.display.clone()),
                        TextColor(P5_BLACK),
                    ));
                    hold_up.spawn((
                        Text::new("!"),
                        TextFont::from_font_size(15.0).with_font(font_assets.display.clone()),
                        TextColor(P5_RED),
                        Node {
                            margin: UiRect::left(Val::Px(2.0)),
                            ..default()
                        },
                    ));
                });

                // Deck Classification Tag (Prison Zebra Border)
                top_action.spawn((
                    Node {
                        padding: UiRect::axes(Val::Px(14.0), Val::Px(4.0)),
                        border: UiRect {
                            left: Val::Px(5.0),
                            top: Val::Px(1.5),
                            right: Val::Px(2.0),
                            bottom: Val::Px(1.5),
                        },
                        ..default()
                    },
                    BackgroundColor(P5_BLACK),
                    BorderColor {
                        left: P5_RED,
                        top: P5_WHITE,
                        right: P5_BORDER,
                        bottom: P5_BORDER,
                    },
                    rot_subtle(),
                    Visibility::default(),
                    InheritedVisibility::default(),
                    SlamEntrance::new(Vec2::new(-200.0, 80.0), 0.2, 0.04),
                )).with_children(|tag| {
                    tag.spawn((
                        Text::new("/// CONCURRENCY ARCHITECTURE // PALACE 01 ///"),
                        TextFont::from_font_size(FONT_CAPTION).with_font(font_assets.display.clone()),
                        TextColor(P5_OFF_WHITE),
                    ));
                });
            });

            // ==============================================================
            // UNIFIED 3D GRAPHIC MANGA TITLE BANNERS (Persona 5 / P5S Style)
            // ==============================================================
            col.spawn((
                Node {
                    flex_direction: FlexDirection::Column,
                    align_items: AlignItems::FlexStart,
                    row_gap: Val::Px(12.0),
                    margin: UiRect::axes(Val::Px(0.0), Val::Px(4.0)),
                    ..default()
                },
                Transform::default(),
                Visibility::default(),
                InheritedVisibility::default(),
            )).with_children(|title_box| {
                // ----------------------------------------------------------
                // BANNER 1: "ON THE R★AD" (Chunky 3D Comic Block with Prismatic Wedges)
                // ----------------------------------------------------------
                title_box.spawn((
                    Node {
                        position_type: PositionType::Relative,
                        padding: UiRect::axes(Val::Px(20.0), Val::Px(8.0)),
                        flex_direction: FlexDirection::Row,
                        align_items: AlignItems::Center,
                        column_gap: Val::Px(14.0),
                        border: UiRect {
                            left: Val::Px(5.5),
                            top: Val::Px(2.5),
                            right: Val::Px(6.0),
                            bottom: Val::Px(6.0),
                        },
                        border_radius: BorderRadius::all(Val::Px(6.0)),
                        ..default()
                    },
                    BackgroundColor(P5_WHITE),
                    BorderColor {
                        left: P5_BLACK,
                        top: P5_BLACK,
                        right: P5_RED,
                        bottom: P5_RED,
                    },
                    Transform::from_rotation(Quat::from_rotation_z(-0.045)),
                    Visibility::default(),
                    InheritedVisibility::default(),
                    SlamEntrance::new(Vec2::new(-300.0, 150.0), -0.35, 0.0),
                    PunkJitter::new(-0.045, 0.025),
                )).with_children(|b1| {
                    // Left Prismatic Neon Cyan Wedge (Reference 1 ITEM wedge)
                    b1.spawn((
                        Node {
                            position_type: PositionType::Absolute,
                            left: Val::Px(-12.0),
                            top: Val::Px(-8.0),
                            width: Val::Px(14.0),
                            height: Val::Px(48.0),
                            border_radius: BorderRadius::all(Val::Px(2.0)),
                            ..default()
                        },
                        BackgroundColor(P5_CYAN),
                        Transform::from_rotation(Quat::from_rotation_z(0.2)),
                    ));

                    // Stamped Corner Ink Splatter on the Title Banner!
                    spawn_corner_ink_splatter(b1, CardCorner::TopLeft, P5_RED, 32.0);

                    // "ON" Inverted Badge
                    b1.spawn((
                        Node {
                            padding: UiRect::axes(Val::Px(10.0), Val::Px(3.0)),
                            border_radius: BorderRadius::all(Val::Px(4.0)),
                            ..default()
                        },
                        BackgroundColor(P5_BLACK),
                    )).with_children(|badge| {
                        badge.spawn((
                            Text::new("ON"),
                            TextFont::from_font_size(32.0).with_font(font_assets.display.clone()),
                            TextColor(P5_WHITE),
                        ));
                    });

                    // "THE" Stylized Text
                    b1.spawn((
                        Text::new("THE"),
                        TextFont::from_font_size(30.0).with_font(font_assets.display.clone()),
                        TextColor(P5_BLACK),
                    ));

                    // "R★AD" Massive Display Block with Embedded Star
                    b1.spawn((
                        Node {
                            flex_direction: FlexDirection::Row,
                            align_items: AlignItems::Center,
                            ..default()
                        },
                    )).with_children(|road| {
                        road.spawn((
                            Text::new("R"),
                            TextFont::from_font_size(46.0).with_font(font_assets.display.clone()),
                            TextColor(P5_RED),
                        ));
                        road.spawn((
                            Text::new("★"),
                            TextFont::from_font_size(36.0).with_font(font_assets.symbols.clone()),
                            TextColor(P5_BLACK),
                            Node {
                                margin: UiRect::axes(Val::Px(2.0), Val::Px(0.0)),
                                ..default()
                            },
                        ));
                        road.spawn((
                            Text::new("AD"),
                            TextFont::from_font_size(46.0).with_font(font_assets.display.clone()),
                            TextColor(P5_BLACK),
                        ));
                    });

                    // Right Prismatic Magenta Shard
                    b1.spawn((
                        Node {
                            position_type: PositionType::Absolute,
                            right: Val::Px(-10.0),
                            bottom: Val::Px(-6.0),
                            width: Val::Px(12.0),
                            height: Val::Px(40.0),
                            border_radius: BorderRadius::all(Val::Px(2.0)),
                            ..default()
                        },
                        BackgroundColor(P5_MAGENTA),
                        Transform::from_rotation(Quat::from_rotation_z(-0.25)),
                    ));
                });

                // ----------------------------------------------------------
                // BANNER 2: "[ TO ] LOCK-F★EEDOM >>" (High-Voltage Crimson Climax Banner)
                // ----------------------------------------------------------
                title_box.spawn((
                    Node {
                        position_type: PositionType::Relative,
                        padding: UiRect::axes(Val::Px(22.0), Val::Px(8.0)),
                        flex_direction: FlexDirection::Row,
                        align_items: AlignItems::Center,
                        column_gap: Val::Px(12.0),
                        border: UiRect {
                            left: Val::Px(6.0),
                            top: Val::Px(2.5),
                            right: Val::Px(6.0),
                            bottom: Val::Px(6.0),
                        },
                        border_radius: BorderRadius::all(Val::Px(8.0)),
                        margin: UiRect::left(Val::Px(20.0)), // Offset staircase effect
                        ..default()
                    },
                    BackgroundColor(P5_RED),
                    BorderColor {
                        left: P5_WHITE,
                        top: P5_WHITE,
                        right: P5_BLACK,
                        bottom: P5_BLACK,
                    },
                    Transform::from_rotation(Quat::from_rotation_z(0.035)),
                    Visibility::default(),
                    InheritedVisibility::default(),
                    SlamEntrance::new(Vec2::new(-240.0, 80.0), 0.25, 0.12),
                    PunkJitter::new(0.035, 0.02),
                )).with_children(|b2| {
                    // Stamped Corner Ink Splatter on Bottom-Right!
                    spawn_corner_ink_splatter(b2, CardCorner::BottomRight, P5_BLACK, 36.0);

                    // "[TO]" Tape Badge
                    b2.spawn((
                        Node {
                            padding: UiRect::axes(Val::Px(8.0), Val::Px(2.0)),
                            border_radius: BorderRadius::all(Val::Px(4.0)),
                            ..default()
                        },
                        BackgroundColor(P5_BLACK),
                    )).with_children(|to| {
                        to.spawn((
                            Text::new("TO"),
                            TextFont::from_font_size(24.0).with_font(font_assets.display.clone()),
                            TextColor(P5_WHITE),
                        ));
                    });

                    // "LOCK"
                    b2.spawn((
                        Text::new("LOCK"),
                        TextFont::from_font_size(44.0).with_font(font_assets.display.clone()),
                        TextColor(P5_WHITE),
                    ));

                    // Electric Dash
                    b2.spawn((
                        Text::new("-"),
                        TextFont::from_font_size(40.0).with_font(font_assets.display.clone()),
                        TextColor(P5_BLACK),
                    ));

                    // "F★EEDOM"
                    b2.spawn((
                        Node {
                            flex_direction: FlexDirection::Row,
                            align_items: AlignItems::Center,
                            ..default()
                        },
                    )).with_children(|freedom| {
                        freedom.spawn((
                            Text::new("F"),
                            TextFont::from_font_size(46.0).with_font(font_assets.display.clone()),
                            TextColor(P5_WHITE),
                        ));
                        freedom.spawn((
                            Text::new("★"),
                            TextFont::from_font_size(36.0).with_font(font_assets.symbols.clone()),
                            TextColor(P5_BLACK),
                            Node {
                                margin: UiRect::axes(Val::Px(2.0), Val::Px(0.0)),
                                ..default()
                            },
                        ));
                        freedom.spawn((
                            Text::new("EEDOM"),
                            TextFont::from_font_size(46.0).with_font(font_assets.display.clone()),
                            TextColor(P5_WHITE),
                        ));
                    });

                    // High-Voltage Chevrons
                    b2.spawn((
                        Text::new(">>"),
                        TextFont::from_font_size(32.0).with_font(font_assets.display.clone()),
                        TextColor(P5_BLACK),
                    ));
                });
            });


            // Accent Divider Slash with Punk "XXX"
            col.spawn((
                Node {
                    flex_direction: FlexDirection::Row,
                    align_items: AlignItems::Center,
                    column_gap: Val::Px(8.0),
                    margin: UiRect::axes(Val::Px(0.0), Val::Px(2.0)),
                    ..default()
                },
                Transform::default(),
                Visibility::default(),
                InheritedVisibility::default(),
                SlamEntrance::new(Vec2::new(-150.0, 0.0), 0.1, 0.24),
            )).with_children(|divider| {
                divider.spawn((
                    Node {
                        width: Val::Px(60.0),
                        height: Val::Px(3.5),
                        ..default()
                    },
                    BackgroundColor(P5_RED),
                ));
                divider.spawn((
                    Text::new("XXX"),
                    TextFont::from_font_size(13.0).with_font(font_assets.display.clone()),
                    TextColor(P5_RED),
                ));
                divider.spawn((
                    Node {
                        width: Val::Px(320.0),
                        height: Val::Px(1.5),
                        ..default()
                    },
                    BackgroundColor(P5_WHITE),
                ));
            });

            // Subtitle Zine Card (Crooked underground zine tag)
            col.spawn((
                Node {
                    padding: UiRect::axes(Val::Px(16.0), Val::Px(7.0)),
                    border: UiRect {
                        left: Val::Px(5.0),
                        top: Val::Px(1.5),
                        right: Val::Px(1.5),
                        bottom: Val::Px(1.5),
                    },
                    ..default()
                },
                BackgroundColor(P5_OFF_BLACK),
                BorderColor {
                    left: P5_RED,
                    top: P5_BORDER,
                    right: P5_BORDER,
                    bottom: P5_BORDER,
                },
                rot_counter(),
                Visibility::default(),
                InheritedVisibility::default(),
                SlamEntrance::new(Vec2::new(-160.0, -40.0), -0.2, 0.26),
            )).with_children(|sub| {
                sub.spawn((
                    Text::new("High-Performance Concurrency, Memory Models & Lock-Free Data Structures"),
                    TextFont::from_font_size(FONT_BODY).with_font(font_assets.sans.clone()),
                    TextColor(P5_OFF_WHITE),
                ));
            });

            // ==============================================================
            // PERSONA 5 INFILTRATION ROUTE MENU STAIRCASE (AGENDA)
            // Inspired by Untouchable Airsoft Shop menu in Reference 3
            // ==============================================================
            col.spawn((
                Node {
                    flex_direction: FlexDirection::Column,
                    row_gap: Val::Px(7.0),
                    margin: UiRect::top(Val::Px(6.0)),
                    ..default()
                },
                Transform::default(),
                Visibility::default(),
                InheritedVisibility::default(),
                SlamEntrance::new(Vec2::new(-140.0, -80.0), 0.15, 0.28),
            )).with_children(|menu| {
                // Menu Cards: (indent, num, title, sub, bg, border_color, fg, corner, splat_col, active, is_climax)
                let menu_cards = [
                    // Card 01: Active White card with Crimson Corner Splatter
                    (0.0, "01", "ON THE ROAD TO LOCK FREEDOM", "[CURRENT ROUTE]", P5_WHITE, BorderColor::all(P5_BLACK), P5_BLACK, CardCorner::TopLeft, P5_RED, true, false),
                    // Card 02: Black card with Crimson Corner Splatter on Top-Right
                    (26.0, "02", "THE COST OF SERIALIZATION", "[MUTEX BOTTLENECK]", P5_BLACK, BorderColor::all(P5_RED), P5_WHITE, CardCorner::TopRight, P5_RED, false, false),
                    // Card 03: Off-Black card with Dark Crimson Corner Splatter on Bottom-Left
                    (52.0, "03", "ATOMIC PRIMITIVES & CAS LOOPS", "[HARDWARE ENGINE]", P5_OFF_BLACK, BorderColor::all(P5_BORDER), P5_OFF_WHITE, CardCorner::BottomLeft, P5_DARK_RED, false, false),
                    // Card 04: Charcoal card with Red Corner Splatter on Top-Left
                    (78.0, "04", "MEMORY ORDERING & CPU CACHES", "[ORDERING FENCES]", P5_CHARCOAL, BorderColor::all(P5_BORDER), P5_LIGHT_GREY, CardCorner::TopLeft, P5_RED, false, false),
                    // Card 05: Climax Amber-Gold card with Black Corner Splatter (Iwai Shop SELL card style!)
                    (104.0, "05", "WAIT-FREE DATA STRUCTURES", "[CLIMAX ROUTE ★]", P5_GOLD, BorderColor::all(P5_BLACK), P5_BLACK, CardCorner::BottomLeft, P5_BLACK, false, true),
                ];

                for (indent, num, title, sub, bg, border_color, fg, corner, splat_color, active, is_climax) in menu_cards {
                    let tilt = if active { -0.04 } else if is_climax { 0.03 } else { -0.02 };
                    let pad_v = if active { 7.0 } else { 5.5 };

                    menu.spawn((
                        Node {
                            position_type: PositionType::Relative,
                            margin: UiRect::left(Val::Px(indent)),
                            width: Val::Px(510.0),
                            padding: UiRect::axes(Val::Px(16.0), Val::Px(pad_v)),
                            flex_direction: FlexDirection::Row,
                            align_items: AlignItems::Center,
                            justify_content: JustifyContent::SpaceBetween,
                            border: UiRect {
                                left: Val::Px(if active { 5.0 } else { 2.5 }),
                                top: Val::Px(1.5),
                                right: Val::Px(if active { 4.0 } else { 2.5 }),
                                bottom: Val::Px(if active { 4.0 } else { 2.5 }),
                            },
                            border_radius: BorderRadius::all(Val::Px(8.0)),
                            ..default()
                        },
                        BackgroundColor(bg),
                        border_color,
                        Transform::from_rotation(Quat::from_rotation_z(tilt)),
                        Visibility::default(),
                        InheritedVisibility::default(),
                        PunkJitter::new(tilt, if active { 0.025 } else { 0.01 }),
                    )).with_children(|card| {
                        // Stamped Corner Ink Splatter on every card (P5 Shop Style)!
                        spawn_corner_ink_splatter(card, corner, splat_color, if active || is_climax { 30.0 } else { 22.0 });

                        // Left Side: Dagger / Star + Number + Title
                        card.spawn((
                            Node {
                                flex_direction: FlexDirection::Row,
                                align_items: AlignItems::Center,
                                column_gap: Val::Px(10.0),
                                ..default()
                            },
                        )).with_children(|left| {
                            if active {
                                left.spawn((
                                    Text::new("▶"),
                                    TextFont::from_font_size(14.0).with_font(font_assets.symbols.clone()),
                                    TextColor(P5_RED),
                                    BobbingCursor::default(),
                                ));
                            } else if is_climax {
                                left.spawn((
                                    Text::new("★"),
                                    TextFont::from_font_size(14.0).with_font(font_assets.symbols.clone()),
                                    TextColor(P5_BLACK),
                                ));
                            } else {
                                left.spawn((
                                    Text::new("•"),
                                    TextFont::from_font_size(12.0).with_font(font_assets.symbols.clone()),
                                    TextColor(P5_MUTED),
                                ));
                            }

                            // Route Number Tag
                            left.spawn((
                                Text::new(num),
                                TextFont::from_font_size(13.0).with_font(font_assets.display.clone()),
                                TextColor(if active { P5_RED } else if is_climax { P5_BLACK } else { P5_MUTED }),
                            ));

                            // Title Text
                            left.spawn((
                                Text::new(title),
                                TextFont::from_font_size(if active || is_climax { 14.0 } else { 12.5 })
                                    .with_font(if active || is_climax { font_assets.display.clone() } else { font_assets.sans.clone() }),
                                TextColor(fg),
                            ));
                        });

                        // Right Side: Context Badge
                        card.spawn((
                            Text::new(sub),
                            TextFont::from_font_size(10.5).with_font(font_assets.sans_heavy.clone()),
                            TextColor(if active { P5_DARK_RED } else if is_climax { P5_DARK_RED } else { P5_MUTED }),
                        ));
                    });
                }
            });
        });


        // ==============================================================
        // RIGHT COLUMN: The Phantom Thieves "Calling Card" (Captivity Manifesto)
        // ==============================================================
        slide.spawn((
            Node {
                flex_direction: FlexDirection::Column,
                width: Val::Px(420.0),
                border: UiRect {
                    left: Val::Px(2.5),
                    top: Val::Px(2.5),
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
            Transform::from_rotation(Quat::from_rotation_z(-0.065)), // -3.7 deg
            Visibility::default(),
            InheritedVisibility::default(),
            SlamEntrance::new(Vec2::new(380.0, 100.0), 0.25, 0.16),
            PunkJitter::new(-0.065, 0.012),
        )).with_children(|card| {
            // Calling Card Header Tape: Prison zebra motif & Confidentiality tag
            card.spawn((
                Node {
                    width: Val::Percent(100.0),
                    padding: UiRect::axes(Val::Px(16.0), Val::Px(10.0)),
                    flex_direction: FlexDirection::Row,
                    justify_content: JustifyContent::SpaceBetween,
                    align_items: AlignItems::Center,
                    border: UiRect {
                        left: Val::Px(0.0),
                        top: Val::Px(0.0),
                        right: Val::Px(0.0),
                        bottom: Val::Px(3.0),
                    },
                    ..default()
                },
                BackgroundColor(P5_CHARCOAL),
                BorderColor::all(P5_RED),
                Transform::default(),
                Visibility::default(),
                InheritedVisibility::default(),
            )).with_children(|header| {
                header.spawn((
                    Node {
                        flex_direction: FlexDirection::Row,
                        align_items: AlignItems::Center,
                        column_gap: Val::Px(6.0),
                        ..default()
                    },
                    Transform::default(),
                    Visibility::default(),
                    InheritedVisibility::default(),
                )).with_children(|h_left| {
                    h_left.spawn((
                        Text::new("★"),
                        TextFont::from_font_size(13.0).with_font(font_assets.symbols.clone()),
                        TextColor(P5_RED),
                    ));
                    h_left.spawn((
                        Text::new("PHANTOM CALLING CARD"),
                        TextFont::from_font_size(FONT_BODY_ICON).with_font(font_assets.sans_heavy.clone()),
                        TextColor(P5_WHITE),
                    ));
                });
                header.spawn((
                    Text::new("/// CLASSIFIED ///"),
                    TextFont::from_font_size(FONT_LABEL_SM).with_font(font_assets.display.clone()),
                    TextColor(P5_RED),
                ));
            });

            // Calling Card Body Content (Letter to the Lord of Mutexes)
            card.spawn((
                Node {
                    flex_direction: FlexDirection::Column,
                    padding: UiRect::all(Val::Px(16.0)),
                    row_gap: Val::Px(10.0),
                    ..default()
                },
                Transform::default(),
                Visibility::default(),
                InheritedVisibility::default(),
            )).with_children(|body| {
                // Target: Captivity
                body.spawn((
                    Node {
                        flex_direction: FlexDirection::Column,
                        row_gap: Val::Px(2.0),
                        border: UiRect::bottom(Val::Px(1.5)),
                        padding: UiRect::bottom(Val::Px(6.0)),
                        ..default()
                    },
                    BorderColor::all(P5_BORDER),
                    Visibility::default(),
                    InheritedVisibility::default(),
                )).with_children(|target| {
                    target.spawn((
                        Text::new("TO: SIR MUTEX OF THE KERNEL"),
                        TextFont::from_font_size(FONT_BODY).with_font(font_assets.display.clone()),
                        TextColor(P5_WHITE),
                    ));
                    target.spawn((
                        Text::new("Captor of concurrent threads and thief of CPU cycles."),
                        TextFont::from_font_size(FONT_LABEL_SM).with_font(font_assets.sans.clone()),
                        TextColor(P5_MUTED),
                    ));
                });

                // Calling Card Letter Text
                body.spawn((
                    Node {
                        padding: UiRect::all(Val::Px(12.0)),
                        border: UiRect {
                            left: Val::Px(4.0),
                            top: Val::Px(1.5),
                            right: Val::Px(1.5),
                            bottom: Val::Px(1.5),
                        },
                        ..default()
                    },
                    BackgroundColor(P5_OFF_BLACK),
                    BorderColor {
                        left: P5_RED,
                        top: P5_BORDER,
                        right: P5_BORDER,
                        bottom: P5_BORDER,
                    },
                    Visibility::default(),
                    InheritedVisibility::default(),
                )).with_children(|letter| {
                    letter.spawn((
                        Text::new("A great sinner of thread captivity. You have locked CPU cores and forced execution into agonizing sleep states for far too long.\n\nTonight, we shall break the chains of mutexes and liberate pure lock-free atomics to the world."),
                        TextFont::from_font_size(FONT_LABEL).with_font(font_assets.serif.clone()),
                        TextColor(P5_OFF_WHITE),
                    ));
                });

                // Sign-off
                body.spawn((
                    Node {
                        width: Val::Percent(100.0),
                        justify_content: JustifyContent::FlexEnd,
                        ..default()
                    },
                    Visibility::default(),
                    InheritedVisibility::default(),
                )).with_children(|sign| {
                    sign.spawn((
                        Text::new("— The Phantom Thieves of Locks"),
                        TextFont::from_font_size(FONT_CAPTION).with_font(font_assets.monospace.clone()),
                        TextColor(P5_LIGHT_GREY),
                    ));
                });

                // Multi-Point Jagged Comic Seal / "TAKE YOUR LOCKS" Stamp
                body.spawn((
                    Node {
                        padding: UiRect::axes(Val::Px(starburst::SEAL_PAD.0), Val::Px(starburst::SEAL_PAD.1)),
                        flex_direction: FlexDirection::Row,
                        justify_content: JustifyContent::Center,
                        align_items: AlignItems::Center,
                        column_gap: Val::Px(6.0),
                        border: starburst::seal_border(),
                        ..default()
                    },
                    BackgroundColor(P5_WHITE),
                    BorderColor {
                        left: P5_BLACK,
                        top: P5_BLACK,
                        right: P5_RED,
                        bottom: P5_RED,
                    },
                    Transform::from_rotation(Quat::from_rotation_z(starburst::SEAL_TILT)), // +6.9 deg
                    Visibility::default(),
                    InheritedVisibility::default(),
                    PunkJitter::new(starburst::SEAL_TILT, 0.02),
                )).with_children(|stamp| {
                    stamp.spawn((
                        Text::new("★"),
                        TextFont::from_font_size(14.0).with_font(font_assets.symbols.clone()),
                        TextColor(P5_BLACK),
                    ));
                    stamp.spawn((
                        Text::new("TAKE YOUR LOCKS"),
                        TextFont::from_font_size(FONT_BODY).with_font(font_assets.display.clone()),
                        TextColor(P5_BLACK),
                    ));
                    stamp.spawn((
                        Text::new("★"),
                        TextFont::from_font_size(14.0).with_font(font_assets.symbols.clone()),
                        TextColor(P5_BLACK),
                    ));
                });
            });
        });
    });
}
