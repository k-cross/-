use bevy::prelude::*;
use crate::theme::colors::*;
use crate::theme::geometry::*;

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
) {
    // Outer Container with Persona 5 layered framing
    parent.spawn((
        Node {
            flex_direction: FlexDirection::Column,
            width: Val::Percent(100.0),
            max_width: Val::Px(820.0),
            border: UiRect::all(Val::Px(2.5)),
            ..default()
        },
        BackgroundColor(CODE_BG),
        BorderColor::all(P5_RED),
    )).with_children(|card| {
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
        )).with_children(|header| {
            // Left: File name badge with red slash
            header.spawn((
                Node {
                    flex_direction: FlexDirection::Row,
                    align_items: AlignItems::Center,
                    column_gap: Val::Px(8.0),
                    ..default()
                },
            )).with_children(|left| {
                left.spawn((
                    Text::new("///"),
                    TextFont::from_font_size(14.0),
                    TextColor(P5_RED),
                ));
                left.spawn((
                    Text::new(file_title),
                    TextFont::from_font_size(14.0),
                    TextColor(P5_WHITE),
                ));
            });

            // Right: Caution / Language Tag
            header.spawn((
                Node {
                    padding: UiRect::axes(Val::Px(10.0), Val::Px(3.0)),
                    border: UiRect::all(Val::Px(1.5)),
                    ..default()
                },
                BackgroundColor(P5_DARK_RED),
                BorderColor::all(P5_WHITE),
                rot_subtle(),
            )).with_children(|tag| {
                tag.spawn((
                    Text::new(language_tag),
                    TextFont::from_font_size(11.0),
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
                ..default()
            },
        )).with_children(|code_body| {
            for (line_idx, tokens) in lines.into_iter().enumerate() {
                code_body.spawn((
                    Node {
                        flex_direction: FlexDirection::Row,
                        align_items: AlignItems::Center,
                        column_gap: Val::Px(16.0),
                        ..default()
                    },
                )).with_children(|row| {
                    // Line number gutter with subtle pointer
                    row.spawn((
                        Node {
                            width: Val::Px(32.0),
                            justify_content: JustifyContent::FlexEnd,
                            ..default()
                        },
                    )).with_children(|gutter| {
                        gutter.spawn((
                            Text::new(format!("{:02}", line_idx + 1)),
                            TextFont::from_font_size(13.0),
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
                    row.spawn((
                        Node {
                            flex_direction: FlexDirection::Row,
                            column_gap: Val::Px(0.0),
                            ..default()
                        },
                    )).with_children(|token_row| {
                        for (text, kind) in tokens {
                            token_row.spawn((
                                Text::new(text),
                                TextFont::from_font_size(14.0),
                                TextColor(kind.color()),
                            ));
                        }
                    });
                });
            }
        });
    });
}
