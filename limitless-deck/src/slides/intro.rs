use crate::slideshow::animation::SlamEntrance;
use crate::slideshow::splatter::{
    CardCorner, spawn_corner_ink_splatter, spawn_ink_blotch, spawn_ui_ink_blotch,
};
use crate::slideshow::{FontAssets, SlideController, SlideState, character};

use crate::theme::colors::*;
use crate::theme::geometry::*;
use crate::theme::starburst;
use crate::theme::typography::*;
use bevy::prelude::*;
use bevy::state::state_scoped::DespawnOnExit;

pub const SELECTED_ROTATION_DEG: f32 = -70.0;
pub const SELECTED_ROTATION_RAD: f32 = SELECTED_ROTATION_DEG * std::f32::consts::PI / 180.0;

#[derive(Resource, Default)]
pub struct IntroSelection {
    pub current: usize,
}

#[derive(Component)]
pub struct IntroMenuCard {
    pub index: usize,
    pub rest_tilt: f32,
    pub is_climax: bool,
    pub current_angle: f32,
    pub target_angle: f32,
}

#[derive(Component, Clone, Copy)]
pub enum IntroCardElement {
    Icon(usize),
    Num(usize),
    Title(usize),
    Sub(usize),
}

pub struct MenuCardData {
    pub indent: f32,
    pub num: &'static str,
    pub title: &'static str,
    pub sub: &'static str,
    pub unselected_bg: Color,
    pub unselected_border: BorderColor,
    pub unselected_fg: Color,
    pub corner: CardCorner,
    pub splat_color: Color,
    pub is_climax: bool,
    pub target_state: SlideState,
}

pub const MENU_CARDS: [MenuCardData; 5] = [
    MenuCardData {
        indent: 0.0,
        num: "01",
        title: "ON THE ROAD TO LOCK FREEDOM",
        sub: "[CURRENT ROUTE]",
        unselected_bg: P5_BLACK,
        unselected_border: BorderColor {
            left: P5_RED,
            top: P5_BORDER,
            right: P5_BORDER,
            bottom: P5_BORDER,
        },
        unselected_fg: P5_WHITE,
        corner: CardCorner::TopLeft,
        splat_color: P5_RED,
        is_climax: false,
        target_state: SlideState::Intro,
    },
    MenuCardData {
        indent: 26.0,
        num: "02",
        title: "SYNCHRONOUS WITH ATOMICS",
        sub: "[THE SIZE RACE]",
        unselected_bg: P5_BLACK,
        unselected_border: BorderColor {
            left: P5_RED,
            top: P5_RED,
            right: P5_RED,
            bottom: P5_RED,
        },
        unselected_fg: P5_WHITE,
        corner: CardCorner::TopRight,
        splat_color: P5_RED,
        is_climax: false,
        target_state: SlideState::MutexBottleneck,
    },
    MenuCardData {
        indent: 52.0,
        num: "03",
        title: "MEMORY SEQUENCING & ABA",
        sub: "[TURN-STAMP RESOLUTION]",
        unselected_bg: P5_OFF_BLACK,
        unselected_border: BorderColor {
            left: P5_BORDER,
            top: P5_BORDER,
            right: P5_BORDER,
            bottom: P5_BORDER,
        },
        unselected_fg: P5_OFF_WHITE,
        corner: CardCorner::BottomLeft,
        splat_color: P5_DARK_RED,
        is_climax: false,
        target_state: SlideState::BoolRep,
    },
    MenuCardData {
        indent: 78.0,
        num: "04",
        title: "HARDWARE CACHE CONTENTION",
        sub: "[APPLE SILICON 128B]",
        unselected_bg: P5_CHARCOAL,
        unselected_border: BorderColor {
            left: P5_BORDER,
            top: P5_BORDER,
            right: P5_BORDER,
            bottom: P5_BORDER,
        },
        unselected_fg: P5_LIGHT_GREY,
        corner: CardCorner::TopLeft,
        splat_color: P5_RED,
        is_climax: false,
        target_state: SlideState::CacheContention,
    },
    MenuCardData {
        indent: 104.0,
        num: "05",
        title: "BRANCHLESS & TAIL LATENCY",
        sub: "[CLIMAX ROUTE ★]",
        unselected_bg: P5_GOLD,
        unselected_border: BorderColor {
            left: P5_BLACK,
            top: P5_BLACK,
            right: P5_BLACK,
            bottom: P5_BLACK,
        },
        unselected_fg: P5_BLACK,
        corner: CardCorner::BottomLeft,
        splat_color: P5_BLACK,
        is_climax: true,
        target_state: SlideState::BranchlessIndex,
    },
];

pub fn spawn_intro_slide(
    mut commands: Commands,
    font_assets: Res<FontAssets>,
    mut selection: ResMut<IntroSelection>,
) {
    selection.current = 0;
    // Slide-scoped high-visibility crimson ink blotch in world space
    let title_blotch = spawn_ink_blotch(
        &mut commands,
        Vec2::new(-280.0, 95.0),
        -65.0,
        P5_RED,
        85.0,
        Vec2::new(1.1, -0.5),
    );
    commands
        .entity(title_blotch)
        .insert(DespawnOnExit(SlideState::Intro));

    // Slide-scoped high-voltage crimson ink spray across the bottom divider
    let crimson_spray = spawn_ink_blotch(
        &mut commands,
        Vec2::new(-160.0, -115.0),
        -64.0,
        P5_BRIGHT_RED,
        42.0,
        Vec2::new(1.4, 0.4),
    );
    commands
        .entity(crimson_spray)
        .insert(DespawnOnExit(SlideState::Intro));

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
                    UiTransform::from_rotation(Rot2::radians(starburst::STARBURST_TILT)),
                    SlamEntrance::new(Vec2::new(-250.0, 120.0), -0.3, 0.0),
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
                        Text::new("KENNY CROSS!"),
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
                    UiTransform::from_rotation(Rot2::radians(-0.045)),
                    SlamEntrance::new(Vec2::new(-300.0, 150.0), -0.35, 0.0),
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
                        UiTransform::from_rotation(Rot2::radians(0.2)),
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
                        UiTransform::from_rotation(Rot2::radians(-0.25)),
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
                    UiTransform::from_rotation(Rot2::radians(0.035)),
                    SlamEntrance::new(Vec2::new(-240.0, 80.0), 0.25, 0.12),
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
                SlamEntrance::new(Vec2::new(-140.0, -80.0), 0.15, 0.28),
            )).with_children(|menu| {
                for (i, card_data) in MENU_CARDS.iter().enumerate() {
                    let is_selected = i == 0;
                    let rest_tilt = if is_selected {
                        -0.04
                    } else if card_data.is_climax {
                        0.03
                    } else {
                        -0.02
                    };
                    let bg = if is_selected {
                        if card_data.is_climax {
                            P5_GOLD
                        } else {
                            P5_WHITE
                        }
                    } else {
                        card_data.unselected_bg
                    };
                    let border_color = if is_selected {
                        if card_data.is_climax {
                            BorderColor::all(P5_RED)
                        } else {
                            BorderColor::all(P5_BLACK)
                        }
                    } else {
                        card_data.unselected_border
                    };
                    let border = if is_selected {
                        UiRect {
                            left: Val::Px(5.0),
                            top: Val::Px(1.5),
                            right: Val::Px(4.0),
                            bottom: Val::Px(4.0),
                        }
                    } else {
                        UiRect {
                            left: Val::Px(2.5),
                            top: Val::Px(1.5),
                            right: Val::Px(2.5),
                            bottom: Val::Px(2.5),
                        }
                    };

                    menu.spawn((
                        Node {
                            position_type: PositionType::Relative,
                            margin: UiRect::left(Val::Px(card_data.indent)),
                            width: Val::Px(510.0),
                            height: Val::Px(38.0),
                            padding: UiRect::axes(Val::Px(16.0), Val::Px(6.0)),
                            flex_direction: FlexDirection::Row,
                            align_items: AlignItems::Center,
                            justify_content: JustifyContent::SpaceBetween,
                            border,
                            border_radius: BorderRadius::all(Val::Px(8.0)),
                            ..default()
                        },
                        BackgroundColor(bg),
                        border_color,
                        UiTransform::from_rotation(Rot2::radians(rest_tilt)),
                        ZIndex(if is_selected { 10 } else { 0 }),
                        IntroMenuCard {
                            index: i,
                            rest_tilt,
                            is_climax: card_data.is_climax,
                            current_angle: rest_tilt,
                            target_angle: if is_selected {
                                SELECTED_ROTATION_RAD
                            } else {
                                rest_tilt
                            },
                        },
                    ))
                    .with_children(|card| {
                        spawn_corner_ink_splatter(
                            card,
                            card_data.corner,
                            card_data.splat_color,
                            if is_selected || card_data.is_climax {
                                30.0
                            } else {
                                22.0
                            },
                        );

                        card.spawn((Node {
                            flex_direction: FlexDirection::Row,
                            align_items: AlignItems::Center,
                            column_gap: Val::Px(10.0),
                            ..default()
                        },))
                        .with_children(|left| {
                            let icon_text = if is_selected {
                                "▶"
                            } else if card_data.is_climax {
                                "★"
                            } else {
                                "•"
                            };
                            let icon_color = if is_selected {
                                P5_RED
                            } else if card_data.is_climax {
                                P5_BLACK
                            } else {
                                P5_MUTED
                            };
                            left.spawn((
                                Text::new(icon_text),
                                TextFont::from_font_size(if is_selected || card_data.is_climax {
                                    14.0
                                } else {
                                    12.0
                                })
                                .with_font(font_assets.symbols.clone()),
                                TextColor(icon_color),
                                IntroCardElement::Icon(i),
                            ));

                            let num_color = if is_selected {
                                P5_RED
                            } else if card_data.is_climax {
                                P5_BLACK
                            } else {
                                P5_MUTED
                            };
                            left.spawn((
                                Text::new(card_data.num),
                                TextFont::from_font_size(13.0)
                                    .with_font(font_assets.display.clone()),
                                TextColor(num_color),
                                IntroCardElement::Num(i),
                            ));

                            let title_color = if is_selected || card_data.is_climax {
                                P5_BLACK
                            } else {
                                card_data.unselected_fg
                            };
                            let title_font = if is_selected || card_data.is_climax {
                                font_assets.display.clone()
                            } else {
                                font_assets.sans.clone()
                            };
                            let title_size = if is_selected || card_data.is_climax {
                                14.0
                            } else {
                                12.5
                            };
                            left.spawn((
                                Text::new(card_data.title),
                                TextFont::from_font_size(title_size).with_font(title_font),
                                TextColor(title_color),
                                IntroCardElement::Title(i),
                            ));
                        });

                        let sub_color = if is_selected || card_data.is_climax {
                            P5_DARK_RED
                        } else {
                            P5_MUTED
                        };
                        card.spawn((
                            Text::new(card_data.sub),
                            TextFont::from_font_size(10.5)
                                .with_font(font_assets.sans_heavy.clone()),
                            TextColor(sub_color),
                            IntroCardElement::Sub(i),
                        ));
                    });
                }

                menu.spawn((Node {
                    flex_direction: FlexDirection::Row,
                    align_items: AlignItems::Center,
                    column_gap: Val::Px(8.0),
                    margin: UiRect::axes(Val::Px(12.0), Val::Px(4.0)),
                    ..default()
                },))
                .with_children(|hint| {
                    hint.spawn((
                        Text::new("★"),
                        TextFont::from_font_size(11.0).with_font(font_assets.symbols.clone()),
                        TextColor(P5_RED),
                    ));
                    hint.spawn((
                        Text::new("[↑ / ↓] SELECT INFILTRATION ROUTE"),
                        TextFont::from_font_size(10.5).with_font(font_assets.sans_heavy.clone()),
                        TextColor(P5_MUTED),
                    ));
                    hint.spawn((
                        Text::new("|"),
                        TextFont::from_font_size(10.5).with_font(font_assets.sans.clone()),
                        TextColor(P5_BORDER),
                    ));
                    hint.spawn((
                        Text::new("[ENTER] INFILTRATE"),
                        TextFont::from_font_size(10.5).with_font(font_assets.sans_heavy.clone()),
                        TextColor(P5_OFF_WHITE),
                    ));
                });
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
            UiTransform::from_rotation(Rot2::radians(-0.065)),
            SlamEntrance::new(Vec2::new(380.0, 100.0), 0.25, 0.16),
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
            )).with_children(|header| {
                header.spawn((
                    Node {
                        flex_direction: FlexDirection::Row,
                        align_items: AlignItems::Center,
                        column_gap: Val::Px(6.0),
                        ..default()
                    },
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
                )).with_children(|letter| {
                    letter.spawn((
                        Text::new("A great sinner of thread captivity. You have locked CPU cores and forced execution into agonizing sleep states for far too long.\n\nTonight, we shall break the chains of blocking synchronization, conquer false sharing, and expose the road to true lock freedom."),
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
                    UiTransform::from_rotation(Rot2::radians(starburst::SEAL_TILT)),
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

pub fn handle_intro_menu_input(
    keys: Res<ButtonInput<KeyCode>>,
    mut selection: ResMut<IntroSelection>,
    mut controller: ResMut<SlideController>,
    mut next_state: ResMut<NextState<SlideState>>,
    mut commands: Commands,
    character_roots: Query<&mut character::PhantomThiefRoot>,
) {
    if keys.just_pressed(KeyCode::ArrowDown) || keys.just_pressed(KeyCode::KeyJ) {
        selection.current = (selection.current + 1) % 5;
    }
    if keys.just_pressed(KeyCode::ArrowUp) || keys.just_pressed(KeyCode::KeyK) {
        selection.current = (selection.current + 4) % 5;
    }
    if keys.just_pressed(KeyCode::Enter) {
        let target_state = MENU_CARDS[selection.current].target_state;
        let (final_state, final_idx) = if target_state == SlideState::Intro {
            (SlideState::MutexBottleneck, 1)
        } else {
            let idx = SlideState::ORDER
                .iter()
                .position(|&s| s == target_state)
                .unwrap_or(0);
            (target_state, idx)
        };

        controller.current_index = final_idx;
        next_state.set(final_state);
        crate::slideshow::animation::spawn_screen_slash(&mut commands);
        crate::slideshow::character::trigger_character_slash(character_roots);
    }
}

pub fn update_intro_menu_visuals(
    selection: Res<IntroSelection>,
    font_assets: Res<FontAssets>,
    mut card_query: Query<(
        &mut IntroMenuCard,
        &mut BackgroundColor,
        &mut BorderColor,
        &mut Node,
        &mut ZIndex,
    )>,
    mut text_query: Query<(&IntroCardElement, &mut Text, &mut TextColor, &mut TextFont)>,
) {
    let sel = selection.current;

    for (mut card, mut bg, mut border_color, mut node, mut z_index) in &mut card_query {
        let is_selected = card.index == sel;
        let data = &MENU_CARDS[card.index];

        if is_selected {
            card.target_angle = SELECTED_ROTATION_RAD;
            *z_index = ZIndex(10);
            *bg = if card.is_climax {
                BackgroundColor(P5_GOLD)
            } else {
                BackgroundColor(P5_WHITE)
            };
            *border_color = if card.is_climax {
                BorderColor::all(P5_RED)
            } else {
                BorderColor::all(P5_BLACK)
            };
            node.border = UiRect {
                left: Val::Px(5.0),
                top: Val::Px(1.5),
                right: Val::Px(4.0),
                bottom: Val::Px(4.0),
            };
        } else {
            card.target_angle = card.rest_tilt;
            *z_index = ZIndex(0);
            *bg = BackgroundColor(data.unselected_bg);
            *border_color = data.unselected_border;
            node.border = UiRect {
                left: Val::Px(2.5),
                top: Val::Px(1.5),
                right: Val::Px(2.5),
                bottom: Val::Px(2.5),
            };
        }
    }

    for (element, mut text, mut color, mut font) in &mut text_query {
        match *element {
            IntroCardElement::Icon(idx) => {
                let is_selected = idx == sel;
                let is_climax = MENU_CARDS[idx].is_climax;
                if is_selected {
                    **text = "▶".to_string();
                    *color = TextColor(P5_RED);
                    *font = TextFont::from_font_size(14.0).with_font(font_assets.symbols.clone());
                } else if is_climax {
                    **text = "★".to_string();
                    *color = TextColor(P5_BLACK);
                    *font = TextFont::from_font_size(14.0).with_font(font_assets.symbols.clone());
                } else {
                    **text = "•".to_string();
                    *color = TextColor(P5_MUTED);
                    *font = TextFont::from_font_size(12.0).with_font(font_assets.symbols.clone());
                }
            }
            IntroCardElement::Num(idx) => {
                let is_selected = idx == sel;
                let is_climax = MENU_CARDS[idx].is_climax;
                if is_selected {
                    *color = TextColor(P5_RED);
                } else if is_climax {
                    *color = TextColor(P5_BLACK);
                } else {
                    *color = TextColor(P5_MUTED);
                }
            }
            IntroCardElement::Title(idx) => {
                let is_selected = idx == sel;
                let is_climax = MENU_CARDS[idx].is_climax;
                let data = &MENU_CARDS[idx];
                if is_selected || is_climax {
                    *color = TextColor(P5_BLACK);
                    *font = TextFont::from_font_size(14.0).with_font(font_assets.display.clone());
                } else {
                    *color = TextColor(data.unselected_fg);
                    *font = TextFont::from_font_size(12.5).with_font(font_assets.sans.clone());
                }
            }
            IntroCardElement::Sub(idx) => {
                let is_selected = idx == sel;
                let is_climax = MENU_CARDS[idx].is_climax;
                if is_selected || is_climax {
                    *color = TextColor(P5_DARK_RED);
                } else {
                    *color = TextColor(P5_MUTED);
                }
            }
        }
    }
}

pub fn animate_intro_menu_cards(
    time: Res<Time>,
    mut query: Query<(&mut UiTransform, &mut IntroMenuCard)>,
) {
    let dt = time.delta_secs();
    const W: f32 = 510.0;
    const H: f32 = 38.0;
    const SPEED: f32 = 18.0;

    for (mut ui_transform, mut card) in &mut query {
        let diff = card.target_angle - card.current_angle;
        if diff.abs() > 0.0005 {
            card.current_angle += diff * (1.0 - (-SPEED * dt).exp());
        } else {
            card.current_angle = card.target_angle;
        }

        let angle = card.current_angle;
        let cos_a = angle.cos();
        let sin_a = angle.sin();

        let delta_x = (W / 2.0) * (cos_a - 1.0) - (H / 2.0) * sin_a;
        let delta_y = (W / 2.0) * sin_a + (H / 2.0) * (cos_a - 1.0);

        ui_transform.translation = Val2::px(delta_x, delta_y);
        ui_transform.rotation = Rot2::radians(angle);
    }
}
