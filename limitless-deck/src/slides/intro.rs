use bevy::prelude::*;
use bevy::state::state_scoped::DespawnOnExit;
use crate::theme::colors::*;
use crate::theme::geometry::*;
use crate::slideshow::SlideState;
use crate::slideshow::animation::{SlamEntrance, PunkJitter, BobbingCursor};
use crate::slideshow::splatter::spawn_ink_blotch;
use crate::theme::cutout;
use crate::theme::starburst;
use crate::theme::typography::*;

pub struct CutoutLetter {
    pub ch: &'static str,
    pub font_size: f32,
    pub bg: Color,
    pub fg: Color,
    pub border: UiRect,
    pub border_color: BorderColor,
    pub tilt: f32,
    pub pad_h: f32,
    pub pad_v: f32,
    pub slam_offset: Vec2,
    pub slam_rot: f32,
    pub delay: f32,
}

pub fn spawn_cutout_letter(builder: &mut ChildSpawnerCommands, letter: CutoutLetter) {
    builder.spawn((
        Node {
            padding: UiRect::axes(Val::Px(letter.pad_h), Val::Px(letter.pad_v)),
            border: letter.border,
            margin: UiRect::axes(Val::Px(cutout::SCRAP_MARGIN), Val::Px(0.0)),
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
            TextFont::from_font_size(letter.font_size),
            TextColor(letter.fg),
        ));
    });
}

pub fn spawn_intro_slide(mut commands: Commands) {
    // Slide-scoped ink blotch behind the title letters
    let title_blotch = spawn_ink_blotch(
        &mut commands,
        Vec2::new(-280.0, 95.0),
        -65.0,
        P5_BLACK,
        75.0,
        Vec2::new(1.1, -0.5),
    );
    commands.entity(title_blotch).insert(DespawnOnExit(SlideState::Intro));

    // Slide-scoped high-voltage crimson ink spray across the bottom divider
    let crimson_spray = spawn_ink_blotch(
        &mut commands,
        Vec2::new(-160.0, -115.0),
        -64.0,
        P5_RED,
        36.0,
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
                        Text::new("💥"),
                        TextFont::from_font_size(12.0),
                        TextColor(P5_RED),
                        Node {
                            margin: UiRect::right(Val::Px(4.0)),
                            ..default()
                        },
                    ));
                    hold_up.spawn((
                        Text::new("HOLD UP!"),
                        TextFont::from_font_size(FONT_BODY_ICON),
                        TextColor(P5_BLACK),
                    ));
                    hold_up.spawn((
                        Text::new("!"),
                        TextFont::from_font_size(15.0),
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
                        TextFont::from_font_size(FONT_CAPTION),
                        TextColor(P5_OFF_WHITE),
                    ));
                });
            });

            // ==============================================================
            // PER-LETTER MAGAZINE CUTOUT RANSOM TITLE
            // "ON THE ROAD TO LOCK FREEDOM"
            // ==============================================================
            col.spawn((
                Node {
                    flex_direction: FlexDirection::Column,
                    align_items: AlignItems::FlexStart,
                    row_gap: Val::Px(10.0),
                    ..default()
                },
                Transform::default(),
                Visibility::default(),
                InheritedVisibility::default(),
            )).with_children(|title_box| {
                // ----------------------------------------------------------
                // ROW 1: "ON"  "THE"  "ROAD" (Clipped letter-by-letter)
                // ----------------------------------------------------------
                title_box.spawn((
                    Node {
                        flex_direction: FlexDirection::Row,
                        align_items: AlignItems::Center,
                        column_gap: Val::Px(0.0),
                        ..default()
                    },
                    Transform::default(),
                    Visibility::default(),
                    InheritedVisibility::default(),
                )).with_children(|row1| {
                    // --- WORD: "ON" ---
                    spawn_cutout_letter(row1, CutoutLetter {
                        ch: "O",
                        font_size: 58.0,
                        bg: P5_WHITE,
                        fg: P5_BLACK,
                        border: UiRect { left: Val::Px(3.0), top: Val::Px(1.5), right: Val::Px(6.0), bottom: Val::Px(6.0) },
                        border_color: BorderColor { left: P5_BLACK, top: P5_BLACK, right: P5_RED, bottom: P5_RED },
                        tilt: 0.18, // +10.3 deg
                        pad_h: 14.0, pad_v: 3.0,
                        slam_offset: Vec2::new(-340.0, 160.0), slam_rot: -0.5, delay: 0.00,
                    });
                    spawn_cutout_letter(row1, CutoutLetter {
                        ch: "N",
                        font_size: 36.0,
                        bg: P5_BLACK,
                        fg: P5_WHITE,
                        border: UiRect { left: Val::Px(4.0), top: Val::Px(1.0), right: Val::Px(1.5), bottom: Val::Px(3.0) },
                        border_color: BorderColor { left: P5_RED, top: P5_WHITE, right: P5_WHITE, bottom: P5_RED },
                        tilt: -0.14, // -8.0 deg
                        pad_h: 8.0, pad_v: 7.0,
                        slam_offset: Vec2::new(-300.0, 140.0), slam_rot: 0.4, delay: 0.03,
                    });

                    // Word Gap Spacer with Torn Masking Tape Scrap
                    row1.spawn((
                        Node {
                            width: Val::Px(cutout::TAPE_WIDTH),
                            height: Val::Px(cutout::TAPE_HEIGHT),
                            margin: UiRect::axes(Val::Px(cutout::TAPE_MARGIN), Val::Px(0.0)),
                            border: UiRect::all(Val::Px(1.0)),
                            ..default()
                        },
                        BackgroundColor(P5_OFF_BLACK),
                        BorderColor::all(P5_BORDER),
                        Transform::from_rotation(Quat::from_rotation_z(-0.35)),
                        Visibility::default(),
                        InheritedVisibility::default(),
                    ));

                    // --- WORD: "THE" ---
                    spawn_cutout_letter(row1, CutoutLetter {
                        ch: "T",
                        font_size: 40.0,
                        bg: P5_BLACK,
                        fg: P5_WHITE,
                        border: UiRect { left: Val::Px(6.0), top: Val::Px(2.0), right: Val::Px(1.5), bottom: Val::Px(1.5) },
                        border_color: BorderColor { left: P5_RED, top: P5_WHITE, right: P5_WHITE, bottom: P5_WHITE },
                        tilt: 0.12, // +6.9 deg
                        pad_h: 8.0, pad_v: 5.0,
                        slam_offset: Vec2::new(-260.0, 120.0), slam_rot: -0.3, delay: 0.06,
                    });
                    spawn_cutout_letter(row1, CutoutLetter {
                        ch: "H",
                        font_size: 34.0,
                        bg: P5_WHITE,
                        fg: P5_BLACK,
                        border: UiRect { left: Val::Px(1.5), top: Val::Px(3.5), right: Val::Px(3.5), bottom: Val::Px(1.5) },
                        border_color: BorderColor { left: P5_BLACK, top: P5_RED, right: P5_BLACK, bottom: P5_BLACK },
                        tilt: -0.22, // -12.6 deg — extreme lean!
                        pad_h: 6.0, pad_v: 8.0,
                        slam_offset: Vec2::new(-240.0, 110.0), slam_rot: 0.45, delay: 0.08,
                    });
                    spawn_cutout_letter(row1, CutoutLetter {
                        ch: "E",
                        font_size: 50.0,
                        bg: P5_OFF_BLACK,
                        fg: P5_OFF_WHITE,
                        border: UiRect { left: Val::Px(2.0), top: Val::Px(2.0), right: Val::Px(5.0), bottom: Val::Px(5.0) },
                        border_color: BorderColor::all(P5_RED),
                        tilt: 0.16, // +9.2 deg
                        pad_h: 12.0, pad_v: 3.0,
                        slam_offset: Vec2::new(-220.0, 100.0), slam_rot: -0.35, delay: 0.10,
                    });

                    // Word Gap Spacer
                    row1.spawn((
                        Node {
                            width: Val::Px(cutout::TAPE_WIDTH),
                            height: Val::Px(cutout::TAPE_HEIGHT),
                            margin: UiRect::axes(Val::Px(cutout::TAPE_MARGIN), Val::Px(0.0)),
                            border: UiRect::all(Val::Px(1.0)),
                            ..default()
                        },
                        BackgroundColor(P5_CHARCOAL),
                        BorderColor::all(P5_RED),
                        Transform::from_rotation(Quat::from_rotation_z(0.25)),
                        Visibility::default(),
                        InheritedVisibility::default(),
                    ));

                    // --- WORD: "ROAD" ---
                    spawn_cutout_letter(row1, CutoutLetter {
                        ch: "R",
                        font_size: 62.0,
                        bg: P5_BLACK,
                        fg: P5_WHITE,
                        border: UiRect { left: Val::Px(7.0), top: Val::Px(1.5), right: Val::Px(2.0), bottom: Val::Px(5.0) },
                        border_color: BorderColor { left: P5_RED, top: P5_WHITE, right: P5_WHITE, bottom: P5_RED },
                        tilt: -0.08, // -4.6 deg
                        pad_h: 15.0, pad_v: 4.0,
                        slam_offset: Vec2::new(-190.0, 90.0), slam_rot: 0.25, delay: 0.12,
                    });
                    spawn_cutout_letter(row1, CutoutLetter {
                        ch: "O",
                        font_size: 38.0,
                        bg: P5_WHITE,
                        fg: P5_BLACK,
                        border: UiRect { left: Val::Px(1.5), top: Val::Px(1.5), right: Val::Px(4.5), bottom: Val::Px(4.5) },
                        border_color: BorderColor { left: P5_BLACK, top: P5_BLACK, right: P5_RED, bottom: P5_RED },
                        tilt: 0.20, // +11.5 deg
                        pad_h: 7.0, pad_v: 6.0,
                        slam_offset: Vec2::new(-170.0, 80.0), slam_rot: -0.4, delay: 0.14,
                    });
                    spawn_cutout_letter(row1, CutoutLetter {
                        ch: "A",
                        font_size: 54.0,
                        bg: P5_BLACK,
                        fg: P5_WHITE,
                        border: UiRect { left: Val::Px(2.0), top: Val::Px(5.0), right: Val::Px(2.0), bottom: Val::Px(2.0) },
                        border_color: BorderColor { left: P5_WHITE, top: P5_RED, right: P5_WHITE, bottom: P5_WHITE },
                        tilt: -0.13, // -7.5 deg
                        pad_h: 13.0, pad_v: 4.0,
                        slam_offset: Vec2::new(-150.0, 70.0), slam_rot: 0.35, delay: 0.16,
                    });
                    spawn_cutout_letter(row1, CutoutLetter {
                        ch: "D",
                        font_size: 42.0,
                        bg: P5_WHITE,
                        fg: P5_BLACK,
                        border: UiRect { left: Val::Px(2.0), top: Val::Px(1.0), right: Val::Px(5.5), bottom: Val::Px(5.5) },
                        border_color: BorderColor { left: P5_BLACK, top: P5_BLACK, right: P5_RED, bottom: P5_RED },
                        tilt: 0.09, // +5.2 deg
                        pad_h: 9.0, pad_v: 7.0,
                        slam_offset: Vec2::new(-130.0, 60.0), slam_rot: -0.2, delay: 0.18,
                    });
                });

                // ----------------------------------------------------------
                // ROW 2: "[ TO ]"  "LOCK"  "FREEDOM" (Clipped letter-by-letter)
                // ----------------------------------------------------------
                title_box.spawn((
                    Node {
                        flex_direction: FlexDirection::Row,
                        align_items: AlignItems::Center,
                        column_gap: Val::Px(0.0),
                        ..default()
                    },
                    Transform::default(),
                    Visibility::default(),
                    InheritedVisibility::default(),
                )).with_children(|row2| {
                    // --- WORD: "[ TO ]" (Ripped Tilted Tape Badge) ---
                    spawn_cutout_letter(row2, CutoutLetter {
                        ch: "T",
                        font_size: 26.0,
                        bg: P5_BLACK,
                        fg: P5_WHITE,
                        border: UiRect { left: Val::Px(3.5), top: Val::Px(1.0), right: Val::Px(1.0), bottom: Val::Px(3.5) },
                        border_color: BorderColor { left: P5_RED, top: P5_WHITE, right: P5_WHITE, bottom: P5_RED },
                        tilt: -0.24, // -13.8 deg — extreme lean!
                        pad_h: 7.0, pad_v: 5.0,
                        slam_offset: Vec2::new(-280.0, 40.0), slam_rot: 0.5, delay: 0.14,
                    });
                    spawn_cutout_letter(row2, CutoutLetter {
                        ch: "O",
                        font_size: 32.0,
                        bg: P5_WHITE,
                        fg: P5_BLACK,
                        border: UiRect { left: Val::Px(1.5), top: Val::Px(3.0), right: Val::Px(3.0), bottom: Val::Px(1.5) },
                        border_color: BorderColor { left: P5_BLACK, top: P5_RED, right: P5_BLACK, bottom: P5_BLACK },
                        tilt: 0.16, // +9.2 deg
                        pad_h: 6.0, pad_v: 6.0,
                        slam_offset: Vec2::new(-260.0, 35.0), slam_rot: -0.35, delay: 0.16,
                    });

                    // Word Gap
                    row2.spawn((
                        Node {
                            width: Val::Px(cutout::TAPE_WIDTH_SM),
                            height: Val::Px(cutout::TAPE_HEIGHT_SM),
                            margin: UiRect::axes(Val::Px(cutout::TAPE_MARGIN_SM), Val::Px(0.0)),
                            border: UiRect::all(Val::Px(1.0)),
                            ..default()
                        },
                        BackgroundColor(P5_OFF_BLACK),
                        BorderColor::all(P5_RED),
                        Transform::from_rotation(Quat::from_rotation_z(-0.25)),
                        Visibility::default(),
                        InheritedVisibility::default(),
                    ));

                    // --- WORD: "LOCK" ---
                    spawn_cutout_letter(row2, CutoutLetter {
                        ch: "L",
                        font_size: 56.0,
                        bg: P5_WHITE,
                        fg: P5_BLACK,
                        border: UiRect { left: Val::Px(2.0), top: Val::Px(1.5), right: Val::Px(6.0), bottom: Val::Px(6.0) },
                        border_color: BorderColor { left: P5_BLACK, top: P5_BLACK, right: P5_RED, bottom: P5_RED },
                        tilt: -0.15, // -8.6 deg
                        pad_h: 14.0, pad_v: 4.0,
                        slam_offset: Vec2::new(-230.0, 30.0), slam_rot: 0.4, delay: 0.18,
                    });
                    spawn_cutout_letter(row2, CutoutLetter {
                        ch: "O",
                        font_size: 36.0,
                        bg: P5_BLACK,
                        fg: P5_WHITE,
                        border: UiRect { left: Val::Px(4.0), top: Val::Px(2.0), right: Val::Px(2.0), bottom: Val::Px(2.0) },
                        border_color: BorderColor { left: P5_RED, top: P5_WHITE, right: P5_WHITE, bottom: P5_WHITE },
                        tilt: 0.12, // +6.9 deg
                        pad_h: 7.0, pad_v: 6.0,
                        slam_offset: Vec2::new(-210.0, 25.0), slam_rot: -0.3, delay: 0.20,
                    });
                    spawn_cutout_letter(row2, CutoutLetter {
                        ch: "C",
                        font_size: 52.0,
                        bg: P5_WHITE,
                        fg: P5_BLACK,
                        border: UiRect { left: Val::Px(1.5), top: Val::Px(1.5), right: Val::Px(5.5), bottom: Val::Px(5.5) },
                        border_color: BorderColor { left: P5_BLACK, top: P5_BLACK, right: P5_RED, bottom: P5_RED },
                        tilt: -0.20, // -11.5 deg
                        pad_h: 13.0, pad_v: 3.0,
                        slam_offset: Vec2::new(-190.0, 20.0), slam_rot: 0.45, delay: 0.22,
                    });
                    spawn_cutout_letter(row2, CutoutLetter {
                        ch: "K",
                        font_size: 44.0,
                        bg: P5_BLACK,
                        fg: P5_WHITE,
                        border: UiRect { left: Val::Px(5.0), top: Val::Px(2.0), right: Val::Px(2.0), bottom: Val::Px(2.0) },
                        border_color: BorderColor { left: P5_RED, top: P5_WHITE, right: P5_WHITE, bottom: P5_WHITE },
                        tilt: 0.10, // +5.7 deg
                        pad_h: 10.0, pad_v: 7.0,
                        slam_offset: Vec2::new(-170.0, 15.0), slam_rot: -0.25, delay: 0.24,
                    });

                    // Word Gap with Electric Chevron
                    row2.spawn((
                        Text::new(">>"),
                        TextFont::from_font_size(30.0),
                        TextColor(P5_RED),
                        Node {
                            margin: UiRect::axes(Val::Px(8.0), Val::Px(0.0)),
                            ..default()
                        },
                    ));

                    // --- WORD: "FREEDOM" (Climax magazine clippings) ---
                    spawn_cutout_letter(row2, CutoutLetter {
                        ch: "F",
                        font_size: 56.0,
                        bg: P5_BLACK,
                        fg: P5_WHITE,
                        border: UiRect { left: Val::Px(7.0), top: Val::Px(1.5), right: Val::Px(1.5), bottom: Val::Px(4.0) },
                        border_color: BorderColor { left: P5_RED, top: P5_WHITE, right: P5_WHITE, bottom: P5_RED },
                        tilt: 0.08, pad_h: 12.0, pad_v: 4.0,
                        slam_offset: Vec2::new(-140.0, 10.0), slam_rot: 0.3, delay: 0.26,
                    });
                    spawn_cutout_letter(row2, CutoutLetter {
                        ch: "R",
                        font_size: 38.0,
                        bg: P5_WHITE,
                        fg: P5_BLACK,
                        border: UiRect { left: Val::Px(1.5), top: Val::Px(1.5), right: Val::Px(4.5), bottom: Val::Px(4.5) },
                        border_color: BorderColor { left: P5_BLACK, top: P5_BLACK, right: P5_RED, bottom: P5_RED },
                        tilt: -0.14, pad_h: 7.0, pad_v: 6.0,
                        slam_offset: Vec2::new(-120.0, 10.0), slam_rot: -0.35, delay: 0.28,
                    });
                    spawn_cutout_letter(row2, CutoutLetter {
                        ch: "E",
                        font_size: 50.0,
                        bg: P5_BLACK,
                        fg: P5_WHITE,
                        border: UiRect { left: Val::Px(2.0), top: Val::Px(5.0), right: Val::Px(2.0), bottom: Val::Px(2.0) },
                        border_color: BorderColor { left: P5_WHITE, top: P5_RED, right: P5_WHITE, bottom: P5_WHITE },
                        tilt: 0.06, pad_h: 11.0, pad_v: 3.0,
                        slam_offset: Vec2::new(-100.0, 10.0), slam_rot: 0.25, delay: 0.30,
                    });
                    spawn_cutout_letter(row2, CutoutLetter {
                        ch: "E",
                        font_size: 34.0,
                        bg: P5_WHITE,
                        fg: P5_BLACK,
                        border: UiRect { left: Val::Px(3.5), top: Val::Px(1.0), right: Val::Px(1.0), bottom: Val::Px(3.5) },
                        border_color: BorderColor { left: P5_RED, top: P5_BLACK, right: P5_BLACK, bottom: P5_RED },
                        tilt: -0.18, pad_h: 6.0, pad_v: 7.0,
                        slam_offset: Vec2::new(-80.0, 10.0), slam_rot: -0.4, delay: 0.32,
                    });
                    spawn_cutout_letter(row2, CutoutLetter {
                        ch: "D",
                        font_size: 48.0,
                        bg: P5_BLACK,
                        fg: P5_WHITE,
                        border: UiRect { left: Val::Px(2.0), top: Val::Px(2.0), right: Val::Px(5.5), bottom: Val::Px(5.5) },
                        border_color: BorderColor { left: P5_WHITE, top: P5_WHITE, right: P5_RED, bottom: P5_RED },
                        tilt: 0.13, pad_h: 12.0, pad_v: 4.0,
                        slam_offset: Vec2::new(-60.0, 10.0), slam_rot: 0.3, delay: 0.34,
                    });
                    spawn_cutout_letter(row2, CutoutLetter {
                        ch: "O",
                        font_size: 40.0,
                        bg: P5_WHITE,
                        fg: P5_BLACK,
                        border: UiRect { left: Val::Px(1.5), top: Val::Px(4.0), right: Val::Px(1.5), bottom: Val::Px(1.5) },
                        border_color: BorderColor { left: P5_BLACK, top: P5_RED, right: P5_BLACK, bottom: P5_BLACK },
                        tilt: -0.11, pad_h: 8.0, pad_v: 5.0,
                        slam_offset: Vec2::new(-40.0, 10.0), slam_rot: -0.25, delay: 0.36,
                    });
                    spawn_cutout_letter(row2, CutoutLetter {
                        ch: "M",
                        font_size: 62.0,
                        bg: P5_BLACK,
                        fg: P5_WHITE,
                        border: UiRect { left: Val::Px(3.0), top: Val::Px(1.5), right: Val::Px(7.0), bottom: Val::Px(7.0) },
                        border_color: BorderColor { left: P5_WHITE, top: P5_WHITE, right: P5_RED, bottom: P5_RED },
                        tilt: 0.05, pad_h: 15.0, pad_v: 5.0,
                        slam_offset: Vec2::new(-20.0, 10.0), slam_rot: 0.2, delay: 0.38,
                    });

                    // Closing Red Chevron
                    row2.spawn((
                        Text::new("<<"),
                        TextFont::from_font_size(30.0),
                        TextColor(P5_RED),
                        Node {
                            margin: UiRect::axes(Val::Px(8.0), Val::Px(0.0)),
                            ..default()
                        },
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
                    TextFont::from_font_size(13.0),
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
                    TextFont::from_font_size(FONT_BODY),
                    TextColor(P5_OFF_WHITE),
                ));
            });

            // ==============================================================
            // PERSONA 5 INFILTRATION ROUTE MENU STAIRCASE (AGENDA)
            // ==============================================================
            col.spawn((
                Node {
                    flex_direction: FlexDirection::Column,
                    row_gap: Val::Px(5.0),
                    margin: UiRect::top(Val::Px(4.0)),
                    ..default()
                },
                Transform::default(),
                Visibility::default(),
                InheritedVisibility::default(),
                SlamEntrance::new(Vec2::new(-140.0, -80.0), 0.15, 0.28),
            )).with_children(|menu| {
                let menu_items = [
                    ("01", "ON THE ROAD TO LOCK FREEDOM", true),
                    ("02", "THE COST OF SERIALIZATION", false),
                    ("03", "ATOMIC PRIMITIVES & CAS LOOPS", false),
                    ("04", "MEMORY ORDERING & CPU CACHE FENCES", false),
                    ("05", "WAIT-FREE DATA STRUCTURES", false),
                ];

                for (num, title, active) in menu_items {
                    if active {
                        // Active P5 Menu Item (Highlighted Cutout with Animated Bobbing Dagger)
                        menu.spawn((
                            Node {
                                padding: UiRect::axes(Val::Px(12.0), Val::Px(4.0)),
                                flex_direction: FlexDirection::Row,
                                align_items: AlignItems::Center,
                                column_gap: Val::Px(10.0),
                                border: UiRect {
                                    left: Val::Px(4.5),
                                    top: Val::Px(1.5),
                                    right: Val::Px(3.5),
                                    bottom: Val::Px(3.5),
                                },
                                ..default()
                            },
                            BackgroundColor(P5_BLACK),
                            BorderColor {
                                left: P5_RED,
                                top: P5_WHITE,
                                right: P5_RED,
                                bottom: P5_RED,
                            },
                            Transform::from_rotation(Quat::from_rotation_z(-0.03)),
                            Visibility::default(),
                            InheritedVisibility::default(),
                        )).with_children(|item| {
                            // Animated Bobbing Dagger Cursor
                            item.spawn((
                                Node {
                                    margin: UiRect::right(Val::Px(4.0)),
                                    ..default()
                                },
                                Text::new("▶"),
                                TextFont::from_font_size(13.0),
                                TextColor(P5_RED),
                                BobbingCursor::default(),
                            ));
                            item.spawn((
                                Text::new(num),
                                TextFont::from_font_size(12.0),
                                TextColor(P5_RED),
                            ));
                            item.spawn((
                                Text::new(title),
                                TextFont::from_font_size(12.5),
                                TextColor(P5_WHITE),
                            ));
                            item.spawn((
                                Text::new("[CURRENT INFILTRATION]"),
                                TextFont::from_font_size(10.0),
                                TextColor(P5_LIGHT_GREY),
                            ));
                        });
                    } else {
                        // Inactive P5 Menu Item (Muted Angular Row)
                        menu.spawn((
                            Node {
                                padding: UiRect::axes(Val::Px(12.0), Val::Px(3.0)),
                                flex_direction: FlexDirection::Row,
                                align_items: AlignItems::Center,
                                column_gap: Val::Px(10.0),
                                ..default()
                            },
                            Transform::from_rotation(Quat::from_rotation_z(-0.02)),
                            Visibility::default(),
                            InheritedVisibility::default(),
                        )).with_children(|item| {
                            item.spawn((
                                Text::new("  "),
                                TextFont::from_font_size(12.0),
                                TextColor(P5_MUTED),
                            ));
                            item.spawn((
                                Text::new(num),
                                TextFont::from_font_size(11.5),
                                TextColor(P5_MUTED),
                            ));
                            item.spawn((
                                Text::new(title),
                                TextFont::from_font_size(11.5),
                                TextColor(P5_MUTED),
                            ));
                        });
                    }
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
                        TextFont::from_font_size(13.0),
                        TextColor(P5_RED),
                    ));
                    h_left.spawn((
                        Text::new("PHANTOM CALLING CARD"),
                        TextFont::from_font_size(FONT_BODY_ICON),
                        TextColor(P5_WHITE),
                    ));
                });
                header.spawn((
                    Text::new("/// CLASSIFIED ///"),
                    TextFont::from_font_size(FONT_LABEL_SM),
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
                        TextFont::from_font_size(FONT_BODY),
                        TextColor(P5_WHITE),
                    ));
                    target.spawn((
                        Text::new("Captor of concurrent threads and thief of CPU cycles."),
                        TextFont::from_font_size(FONT_LABEL_SM),
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
                        TextFont::from_font_size(FONT_LABEL),
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
                        TextFont::from_font_size(FONT_CAPTION),
                        TextColor(P5_LIGHT_GREY),
                    ));
                });

                // Multi-Point Jagged Comic Seal / "TAKE YOUR LOCKS" Stamp
                body.spawn((
                    Node {
                        padding: UiRect::axes(Val::Px(starburst::SEAL_PAD.0), Val::Px(starburst::SEAL_PAD.1)),
                        justify_content: JustifyContent::Center,
                        align_items: AlignItems::Center,
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
                        Text::new("💥 TAKE YOUR LOCKS 💥"),
                        TextFont::from_font_size(FONT_BODY),
                        TextColor(P5_BLACK),
                    ));
                });
            });
        });
    });
}
