use bevy::prelude::*;
use crate::theme::colors::*;
use crate::theme::geometry::*;
use crate::theme::typography::*;
use super::SlideController;

#[derive(Component)]
pub struct SlideCounterText;

#[derive(Component)]
pub struct ProgressBarFill;

#[derive(Component)]
pub struct RotatingStar {
    pub speed: f32,
}

#[derive(Component)]
pub struct PulsingAlarm {
    pub speed: f32,
}

pub fn setup_hud(mut commands: Commands, controller: Res<SlideController>) {
    // Root HUD node (non-blocking, full-screen overlay)
    commands.spawn((
        Node {
            position_type: PositionType::Absolute,
            width: Val::Percent(100.0),
            height: Val::Percent(100.0),
            flex_direction: FlexDirection::Column,
            justify_content: JustifyContent::SpaceBetween,
            padding: UiRect::axes(Val::Px(28.0), Val::Px(20.0)),
            ..default()
        },
        Transform::default(),
        Visibility::default(),
        InheritedVisibility::default(),
    )).with_children(|root| {
        // ==============================================================
        // TOP BAR: Persona 5 Calendar & High-Security Alert Widgets
        // ==============================================================
        root.spawn((
            Node {
                width: Val::Percent(100.0),
                flex_direction: FlexDirection::Row,
                justify_content: JustifyContent::SpaceBetween,
                align_items: AlignItems::FlexStart,
                ..default()
            },
            Transform::default(),
            Visibility::default(),
            InheritedVisibility::default(),
        )).with_children(|top_bar| {
            // Top Left: Iconic Persona 5 Calendar & Prison Infiltration HUD
            top_bar.spawn((
                Node {
                    flex_direction: FlexDirection::Column,
                    row_gap: Val::Px(5.0),
                    padding: UiRect::axes(Val::Px(16.0), Val::Px(8.0)),
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
                rot_subtle(),
            )).with_children(|cal| {
                // Top row: Date & Infiltration Time
                cal.spawn(Node {
                    flex_direction: FlexDirection::Row,
                    align_items: AlignItems::Center,
                    column_gap: Val::Px(8.0),
                    ..default()
                }).with_children(|row| {
                    // Date Stamp
                    row.spawn((
                        Text::new("09 / 23"),
                        TextFont::from_font_size(FONT_HUD_DATE),
                        TextColor(P5_WHITE),
                    ));
                    // Weather / Time Tag
                    row.spawn((
                        Node {
                            padding: UiRect::axes(Val::Px(6.0), Val::Px(2.0)),
                            border: UiRect::all(Val::Px(1.0)),
                            ..default()
                        },
                        BackgroundColor(P5_CHARCOAL),
                        BorderColor::all(P5_BORDER),
                    )).with_children(|w| {
                        w.spawn((
                            Text::new("AFTER SCHOOL"),
                            TextFont::from_font_size(FONT_TAG),
                            TextColor(P5_LIGHT_GREY),
                        ));
                    });
                });

                // Palace & Pulsing Security Alert Gauge
                cal.spawn(Node {
                    flex_direction: FlexDirection::Row,
                    align_items: AlignItems::Center,
                    column_gap: Val::Px(6.0),
                    ..default()
                }).with_children(|sub| {
                    sub.spawn((
                        Text::new("PALACE: KERNEL // ALERT:"),
                        TextFont::from_font_size(FONT_LABEL_SM),
                        TextColor(P5_MUTED),
                    ));
                    // Segmented Alarm Meter
                    sub.spawn((
                        Text::new("[▮▮▮▮▮▮▮▮▮▯ 99%]"),
                        TextFont::from_font_size(FONT_LABEL_SM),
                        TextColor(P5_RED),
                        PulsingAlarm { speed: 8.0 },
                    ));
                    sub.spawn((
                        Text::new("[! TRESPASSER !]"),
                        TextFont::from_font_size(FONT_TAG_SM),
                        TextColor(P5_WHITE),
                    ));
                });
            });

            // Top Right: Persona 5 Target Intel Badge
            top_bar.spawn((
                Node {
                    padding: UiRect::axes(Val::Px(16.0), Val::Px(8.0)),
                    flex_direction: FlexDirection::Column,
                    align_items: AlignItems::FlexEnd,
                    row_gap: Val::Px(4.0),
                    border: UiRect {
                        left: Val::Px(2.0),
                        top: Val::Px(2.0),
                        right: Val::Px(4.5),
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
                rot_counter(),
            )).with_children(|status| {
                status.spawn((
                    Text::new("TARGET: CORRUPT LORD OF MUTEXES"),
                    TextFont::from_font_size(FONT_CAPTION_LG),
                    TextColor(P5_WHITE),
                ));
                status.spawn((
                    Text::new("CHAINS OF SERIALIZATION: BREAKING"),
                    TextFont::from_font_size(FONT_LABEL_SM),
                    TextColor(P5_LIGHT_GREY),
                ));
            });
        });

        // ==============================================================
        // BOTTOM BAR: Slide Progress & "TAKE YOUR TIME" Action Strip
        // ==============================================================
        root.spawn((
            Node {
                width: Val::Percent(100.0),
                flex_direction: FlexDirection::Column,
                row_gap: Val::Px(8.0),
                ..default()
            },
            Transform::default(),
            Visibility::default(),
            InheritedVisibility::default(),
        )).with_children(|bottom_container| {
            // Control banner & Slide counter
            bottom_container.spawn((
                Node {
                    width: Val::Percent(100.0),
                    flex_direction: FlexDirection::Row,
                    justify_content: JustifyContent::SpaceBetween,
                    align_items: AlignItems::Center,
                    ..default()
                },
                Transform::default(),
                Visibility::default(),
                InheritedVisibility::default(),
            )).with_children(|banner| {
                // Left: Slide Counter Cutout Badge
                banner.spawn((
                    Node {
                        padding: UiRect::axes(Val::Px(16.0), Val::Px(6.0)),
                        flex_direction: FlexDirection::Row,
                        align_items: AlignItems::Center,
                        column_gap: Val::Px(8.0),
                        border: UiRect {
                            left: Val::Px(4.5),
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
                    rot_primary(),
                )).with_children(|badge| {
                    badge.spawn((
                        Text::new("///"),
                        TextFont::from_font_size(FONT_HUD_STAR),
                        TextColor(P5_RED),
                    ));
                    badge.spawn((
                        Text::new(format!("SLIDE {:02} / {:02}", controller.current_index + 1, controller.total_slides)),
                        TextFont::from_font_size(FONT_HUD_COUNTER),
                        TextColor(P5_WHITE),
                        SlideCounterText,
                    ));
                });

                // Center / Right: Persona 5 "TAKE YOUR TIME" & Combat Key Prompts
                banner.spawn((
                    Node {
                        padding: UiRect::axes(Val::Px(16.0), Val::Px(6.0)),
                        flex_direction: FlexDirection::Row,
                        align_items: AlignItems::Center,
                        column_gap: Val::Px(12.0),
                        border: UiRect {
                            left: Val::Px(2.0),
                            top: Val::Px(2.0),
                            right: Val::Px(4.5),
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
                    rot_counter(),
                )).with_children(|prompt| {
                    // Animated Persona 5 "Take Your Time" rotating star
                    prompt.spawn((
                        Text::new("★"),
                        TextFont::from_font_size(FONT_HUD_STAR),
                        TextColor(P5_RED),
                        RotatingStar { speed: 2.5 },
                    ));
                    prompt.spawn((
                        Text::new("TAKE YOUR TIME"),
                        TextFont::from_font_size(FONT_HUD_LABEL),
                        TextColor(P5_LIGHT_GREY),
                    ));
                    prompt.spawn((
                        Text::new("|"),
                        TextFont::from_font_size(FONT_HUD_LABEL),
                        TextColor(P5_BORDER),
                    ));
                    prompt.spawn((
                        Text::new("[SPACE / →] EXECUTE ATTACK"),
                        TextFont::from_font_size(FONT_HUD_PROMPT),
                        TextColor(P5_WHITE),
                    ));
                    prompt.spawn((
                        Text::new("★"),
                        TextFont::from_font_size(FONT_HUD_LABEL),
                        TextColor(P5_RED),
                    ));
                    prompt.spawn((
                        Text::new("[←] BATON PASS"),
                        TextFont::from_font_size(FONT_HUD_PROMPT),
                        TextColor(P5_WHITE),
                    ));
                    prompt.spawn((
                        Text::new("★"),
                        TextFont::from_font_size(FONT_HUD_LABEL),
                        TextColor(P5_RED),
                    ));
                    prompt.spawn((
                        Text::new("[F] ALL-OUT ATTACK (FULLSCREEN)"),
                        TextFont::from_font_size(FONT_HUD_PROMPT),
                        TextColor(P5_WHITE),
                    ));
                });
            });

            // Sleek Progress Track with Crimson Fill & White Border
            bottom_container.spawn((
                Node {
                    width: Val::Percent(100.0),
                    height: Val::Px(4.0),
                    border: UiRect::all(Val::Px(1.0)),
                    ..default()
                },
                BackgroundColor(P5_BLACK),
                BorderColor::all(P5_BORDER),
            )).with_children(|track| {
                let progress_pct = if controller.total_slides > 0 {
                    ((controller.current_index + 1) as f32 / controller.total_slides as f32) * 100.0
                } else {
                    100.0
                };
                track.spawn((
                    Node {
                        width: Val::Percent(progress_pct),
                        height: Val::Percent(100.0),
                        ..default()
                    },
                    BackgroundColor(P5_RED),
                    ProgressBarFill,
                ));
            });
        });
    });
}

pub fn update_hud(
    controller: Res<SlideController>,
    mut text_query: Query<&mut Text, With<SlideCounterText>>,
    mut bar_query: Query<&mut Node, With<ProgressBarFill>>,
) {
    if controller.is_changed() {
        for mut text in &mut text_query {
            **text = format!("SLIDE {:02} / {:02}", controller.current_index + 1, controller.total_slides);
        }
        for mut node in &mut bar_query {
            let progress_pct = if controller.total_slides > 0 {
                ((controller.current_index + 1) as f32 / controller.total_slides as f32) * 100.0
            } else {
                100.0
            };
            node.width = Val::Percent(progress_pct);
        }
    }
}

pub fn animate_hud(
    time: Res<Time>,
    mut star_query: Query<(&mut Transform, &RotatingStar)>,
    mut alarm_query: Query<(&mut TextColor, &PulsingAlarm)>,
) {
    let dt = time.delta_secs();
    let elapsed = time.elapsed_secs();

    for (mut transform, star) in &mut star_query {
        transform.rotate_z(star.speed * dt);
    }

    for (mut color, alarm) in &mut alarm_query {
        let pulse = (elapsed * alarm.speed).sin().abs();
        let r = 0.8 + 0.2 * pulse;
        color.0 = Color::srgb(r, 0.0, 0.05);
    }
}
