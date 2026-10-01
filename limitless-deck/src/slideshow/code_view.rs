use crate::theme::colors::*;
use crate::theme::geometry::*;
use crate::theme::typography::*;
use bevy::input::mouse::AccumulatedMouseScroll;
use bevy::prelude::*;
use bevy::ui::ScrollPosition;

#[derive(Component)]
pub struct CodeBlockScroll;

#[derive(Component)]
pub struct BlinkingLine;

const BLINK_HZ: f32 = 1.5;
const BLINK_PEAK_ALPHA: f32 = 0.75;

pub fn blink_alert_lines(
    time: Res<Time>,
    mut lines: Query<&mut BackgroundColor, With<BlinkingLine>>,
) {
    let wave = (time.elapsed_secs() * BLINK_HZ * std::f32::consts::TAU).sin();
    let intensity = ((wave * 3.0).clamp(-1.0, 1.0) * 0.5 + 0.5) * BLINK_PEAK_ALPHA;
    for mut background in &mut lines {
        background.0 = P5_RED.with_alpha(intensity);
    }
}

pub fn scroll_code_blocks(
    mouse_scroll: Res<AccumulatedMouseScroll>,
    keyboard: Res<ButtonInput<KeyCode>>,
    mut query: Query<&mut ScrollPosition, With<CodeBlockScroll>>,
) {
    let mut delta_y = -mouse_scroll.delta.y * 20.0;
    if keyboard.pressed(KeyCode::KeyJ) || keyboard.pressed(KeyCode::ArrowDown) {
        delta_y += 8.0;
    }
    if keyboard.pressed(KeyCode::KeyK) || keyboard.pressed(KeyCode::ArrowUp) {
        delta_y -= 8.0;
    }
    if delta_y != 0.0 {
        for mut pos in &mut query {
            pos.y = (pos.y + delta_y).max(0.0);
        }
    }
}

/// Token type for syntax highlighting in presentation code blocks
#[derive(Clone, Copy)]
#[allow(dead_code)]
pub enum TokenKind {
    Keyword,
    Type,
    Function,
    String,
    Comment,
    Number,
    Plain,
    Alert,
}

impl TokenKind {
    pub fn color(&self) -> Color {
        match self {
            TokenKind::Keyword => CODE_KEYWORD,
            TokenKind::Type => CODE_TYPE,
            TokenKind::Function => CODE_FUNCTION,
            TokenKind::String => CODE_STRING,
            TokenKind::Comment => CODE_COMMENT,
            TokenKind::Number => CODE_NUMBER,
            TokenKind::Plain => CODE_TEXT,
            TokenKind::Alert => P5_WHITE,
        }
    }
}

/// Helper to spawn a styled code block that balances crystal-clear readability
/// with the aggressive, stylized Persona 5 graphic framing.
pub fn spawn_styled_code_block(
    parent: &mut ChildSpawnerCommands,
    file_title: &str,
    language_tag: &str,
    lines: Vec<Vec<(&str, TokenKind)>>,
    mono_font: Option<Handle<Font>>,
) {
    let font_gutter = if let Some(ref f) = mono_font {
        TextFont::from_font_size(FONT_CODE).with_font(f.clone())
    } else {
        TextFont::from_font_size(FONT_CODE)
    };
    let font_token = if let Some(ref f) = mono_font {
        TextFont::from_font_size(FONT_CODE).with_font(f.clone())
    } else {
        TextFont::from_font_size(FONT_CODE)
    };

    // Outer Container with Persona 5 layered framing
    parent
        .spawn((
            Node {
                flex_direction: FlexDirection::Column,
                width: Val::Percent(100.0),
                max_width: Val::Px(820.0),
                border: UiRect::all(Val::Px(2.5)),
                ..default()
            },
            BackgroundColor(CODE_BG),
            BorderColor::all(P5_RED),
        ))
        .with_children(|card| {
            // --- Persona 5 Terminal Header Tab ---
            card.spawn((
                Node {
                    width: Val::Percent(100.0),
                    padding: UiRect::axes(Val::Px(16.0), Val::Px(8.0)),
                    flex_direction: FlexDirection::Row,
                    justify_content: JustifyContent::SpaceBetween,
                    align_items: AlignItems::Center,
                    border: UiRect::bottom(Val::Px(2.0)),
                    ..default()
                },
                BackgroundColor(P5_BLACK),
                BorderColor::all(P5_RED),
            ))
            .with_children(|header| {
                // Left: File name badge with red slash
                header
                    .spawn((Node {
                        flex_direction: FlexDirection::Row,
                        align_items: AlignItems::Center,
                        column_gap: Val::Px(8.0),
                        ..default()
                    },))
                    .with_children(|left| {
                        left.spawn((
                            Text::new("///"),
                            TextFont::from_font_size(FONT_CODE),
                            TextColor(P5_RED),
                        ));
                        left.spawn((
                            Text::new(file_title),
                            TextFont::from_font_size(FONT_CODE),
                            TextColor(P5_WHITE),
                        ));
                    });

                // Right: Caution / Language Tag
                header
                    .spawn((
                        Node {
                            padding: UiRect::axes(Val::Px(10.0), Val::Px(3.0)),
                            border: UiRect::all(Val::Px(1.5)),
                            ..default()
                        },
                        BackgroundColor(P5_DARK_RED),
                        BorderColor::all(P5_WHITE),
                        rot_subtle(),
                    ))
                    .with_children(|tag| {
                        tag.spawn((
                            Text::new(language_tag),
                            TextFont::from_font_size(FONT_LABEL_SM),
                            TextColor(P5_WHITE),
                        ));
                    });
            });

            // --- Code Content Area ---
            // Laid out strictly rectilinear for maximum readability without perspective distortion
            card.spawn((
                Node {
                    flex_direction: FlexDirection::Column,
                    padding: UiRect::all(Val::Px(18.0)),
                    row_gap: Val::Px(6.0),
                    max_height: Val::Px(420.0),
                    overflow: Overflow::scroll_y(),
                    ..default()
                },
                ScrollPosition::default(),
                CodeBlockScroll,
            ))
            .with_children(|code_body| {
                for (line_idx, tokens) in lines.into_iter().enumerate() {
                    let is_alert = tokens
                        .iter()
                        .any(|(_, kind)| matches!(kind, TokenKind::Alert));
                    let mut row_entity = code_body.spawn((Node {
                        flex_direction: FlexDirection::Row,
                        align_items: AlignItems::Center,
                        column_gap: Val::Px(16.0),
                        ..default()
                    },));
                    if is_alert {
                        row_entity.insert((BackgroundColor(P5_RED.with_alpha(0.0)), BlinkingLine));
                    }
                    row_entity.with_children(|row| {
                        // Line number gutter with subtle pointer
                        row.spawn((Node {
                            width: Val::Px(32.0),
                            justify_content: JustifyContent::FlexEnd,
                            ..default()
                        },))
                            .with_children(|gutter| {
                                gutter.spawn((
                                    Text::new(format!("{:02}", line_idx + 1)),
                                    font_gutter.clone(),
                                    TextColor(P5_MUTED),
                                ));
                            });

                        // Separator bar
                        row.spawn((
                            Node {
                                width: Val::Px(1.5),
                                height: Val::Px(14.0),
                                ..default()
                            },
                            BackgroundColor(P5_BORDER),
                        ));

                        // Token spans
                        row.spawn((Node {
                            flex_direction: FlexDirection::Row,
                            column_gap: Val::Px(0.0),
                            ..default()
                        },))
                            .with_children(|token_row| {
                                for (text, kind) in tokens {
                                    token_row.spawn((
                                        Text::new(text),
                                        font_token.clone(),
                                        TextColor(kind.color()),
                                    ));
                                }
                            });
                    });
                }
            });
        });
}
