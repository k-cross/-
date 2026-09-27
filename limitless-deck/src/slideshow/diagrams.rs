use crate::theme::colors::*;
use crate::theme::typography::*;
use bevy::prelude::*;

/// Spawns a horizontal or vertical sequence flow diagram representing thread execution stages.
pub fn spawn_flow_diagram(
    parent: &mut ChildSpawnerCommands,
    font: Handle<Font>,
    label: &str,
    label_color: Color,
    steps: &[&str],
    highlight_idx: Option<usize>,
) {
    parent
        .spawn((
            Node {
                flex_direction: FlexDirection::Column,
                align_items: AlignItems::FlexStart,
                padding: UiRect::all(Val::Px(10.0)),
                row_gap: Val::Px(6.0),
                border: UiRect {
                    left: Val::Px(3.0),
                    top: Val::Px(1.5),
                    right: Val::Px(1.5),
                    bottom: Val::Px(1.5),
                },
                ..default()
            },
            BackgroundColor(P5_BLACK),
            BorderColor {
                left: label_color,
                top: P5_BORDER,
                right: P5_BORDER,
                bottom: P5_BORDER,
            },
        ))
        .with_children(|card| {
            card.spawn((
                Text::new(label),
                TextFont::from_font_size(FONT_LABEL).with_font(font.clone()),
                TextColor(label_color),
            ));

            card.spawn(Node {
                flex_direction: FlexDirection::Row,
                align_items: AlignItems::Center,
                flex_wrap: FlexWrap::Wrap,
                column_gap: Val::Px(5.0),
                row_gap: Val::Px(4.0),
                ..default()
            })
            .with_children(|row| {
                for (i, step) in steps.iter().enumerate() {
                    let is_highlighted = highlight_idx == Some(i);
                    let bg_color = if is_highlighted { P5_RED } else { P5_CHARCOAL };
                    let text_color = if is_highlighted {
                        P5_WHITE
                    } else {
                        P5_OFF_WHITE
                    };
                    let border_col = if is_highlighted { P5_WHITE } else { P5_BORDER };

                    row.spawn((
                        Node {
                            padding: UiRect::axes(Val::Px(7.0), Val::Px(4.0)),
                            border: UiRect::all(Val::Px(1.5)),
                            ..default()
                        },
                        BackgroundColor(bg_color),
                        BorderColor::all(border_col),
                    ))
                    .with_children(|b| {
                        b.spawn((
                            Text::new(*step),
                            TextFont::from_font_size(FONT_LABEL_SM).with_font(font.clone()),
                            TextColor(text_color),
                        ));
                    });

                    if i < steps.len() - 1 {
                        row.spawn((
                            Text::new("→"),
                            TextFont::from_font_size(FONT_LABEL).with_font(font.clone()),
                            TextColor(P5_MUTED),
                        ));
                    }
                }
            });
        });
}

/// Spawns a styled mathematical expression panel with color-coded variables and operands.
pub fn spawn_math_formula(
    parent: &mut ChildSpawnerCommands,
    font: Handle<Font>,
    title: &str,
    segments: &[(&str, Color)],
) {
    parent
        .spawn((
            Node {
                flex_direction: FlexDirection::Column,
                padding: UiRect::all(Val::Px(12.0)),
                row_gap: Val::Px(6.0),
                border: UiRect {
                    left: Val::Px(4.0),
                    top: Val::Px(1.5),
                    right: Val::Px(1.5),
                    bottom: Val::Px(1.5),
                },
                ..default()
            },
            BackgroundColor(P5_BLACK),
            BorderColor {
                left: P5_RED,
                top: P5_BORDER,
                right: P5_BORDER,
                bottom: P5_BORDER,
            },
        ))
        .with_children(|card| {
            if !title.is_empty() {
                card.spawn((
                    Text::new(title),
                    TextFont::from_font_size(FONT_LABEL_SM).with_font(font.clone()),
                    TextColor(P5_RED),
                ));
            }

            card.spawn(Node {
                flex_direction: FlexDirection::Row,
                align_items: AlignItems::Center,
                flex_wrap: FlexWrap::Wrap,
                column_gap: Val::Px(2.0),
                ..default()
            })
            .with_children(|row| {
                for (text, color) in segments {
                    row.spawn((
                        Text::new(*text),
                        TextFont::from_font_size(FONT_BODY_SM).with_font(font.clone()),
                        TextColor(*color),
                    ));
                }
            });
        });
}

pub struct BarEntry {
    pub label: &'static str,
    pub value_text: &'static str,
    pub fraction: f32, // 0.0 to 1.0
    pub color: Color,
}

/// Spawns a horizontal bar chart with labels and percentage/value readouts.
pub fn spawn_bar_chart(
    parent: &mut ChildSpawnerCommands,
    font: Handle<Font>,
    title: &str,
    max_width: f32,
    entries: Vec<BarEntry>,
) {
    parent
        .spawn((
            Node {
                flex_direction: FlexDirection::Column,
                padding: UiRect::all(Val::Px(12.0)),
                row_gap: Val::Px(8.0),
                border: UiRect {
                    left: Val::Px(4.0),
                    top: Val::Px(1.5),
                    right: Val::Px(1.5),
                    bottom: Val::Px(1.5),
                },
                ..default()
            },
            BackgroundColor(P5_BLACK),
            BorderColor {
                left: P5_RED,
                top: P5_BORDER,
                right: P5_BORDER,
                bottom: P5_BORDER,
            },
        ))
        .with_children(|card| {
            card.spawn((
                Text::new(title),
                TextFont::from_font_size(FONT_BODY_LG).with_font(font.clone()),
                TextColor(P5_WHITE),
            ));

            for entry in entries {
                card.spawn(Node {
                    flex_direction: FlexDirection::Column,
                    row_gap: Val::Px(3.0),
                    ..default()
                })
                .with_children(|item| {
                    item.spawn(Node {
                        flex_direction: FlexDirection::Row,
                        justify_content: JustifyContent::SpaceBetween,
                        width: Val::Percent(100.0),
                        ..default()
                    })
                    .with_children(|header| {
                        header.spawn((
                            Text::new(entry.label),
                            TextFont::from_font_size(FONT_LABEL_SM).with_font(font.clone()),
                            TextColor(P5_LIGHT_GREY),
                        ));
                        header.spawn((
                            Text::new(entry.value_text),
                            TextFont::from_font_size(FONT_LABEL_SM).with_font(font.clone()),
                            TextColor(entry.color),
                        ));
                    });

                    // Bar Track
                    item.spawn((
                        Node {
                            width: Val::Px(max_width),
                            height: Val::Px(14.0),
                            border: UiRect::all(Val::Px(1.0)),
                            ..default()
                        },
                        BackgroundColor(P5_CHARCOAL),
                        BorderColor::all(P5_BORDER),
                    ))
                    .with_children(|track| {
                        let fill_width = (entry.fraction * max_width).clamp(2.0, max_width);
                        track.spawn((
                            Node {
                                width: Val::Px(fill_width),
                                height: Val::Percent(100.0),
                                ..default()
                            },
                            BackgroundColor(entry.color),
                        ));
                    });
                });
            }
        });
}

pub struct SlotState {
    pub label: &'static str,
    pub value: &'static str,
    pub state: &'static str,
    pub is_highlighted: bool,
    pub is_error: bool,
}

/// Spawns a graphic ring buffer state diagram showing slot cells with generation stamps and cursor pointers.
pub fn spawn_ring_buffer_diagram(
    parent: &mut ChildSpawnerCommands,
    font: Handle<Font>,
    title: &str,
    slots: Vec<SlotState>,
    read_idx: usize,
    write_idx: usize,
) {
    parent
        .spawn((
            Node {
                flex_direction: FlexDirection::Column,
                padding: UiRect::all(Val::Px(12.0)),
                row_gap: Val::Px(8.0),
                align_items: AlignItems::Center,
                border: UiRect {
                    left: Val::Px(4.0),
                    top: Val::Px(1.5),
                    right: Val::Px(1.5),
                    bottom: Val::Px(1.5),
                },
                ..default()
            },
            BackgroundColor(P5_BLACK),
            BorderColor {
                left: P5_RED,
                top: P5_BORDER,
                right: P5_BORDER,
                bottom: P5_BORDER,
            },
        ))
        .with_children(|card| {
            if !title.is_empty() {
                card.spawn((
                    Text::new(title),
                    TextFont::from_font_size(FONT_LABEL_SM).with_font(font.clone()),
                    TextColor(P5_RED),
                ));
            }

            // Slots Row
            card.spawn(Node {
                flex_direction: FlexDirection::Row,
                column_gap: Val::Px(6.0),
                ..default()
            })
            .with_children(|row| {
                for slot in &slots {
                    let bg_color = if slot.is_error {
                        P5_DARK_RED
                    } else if slot.is_highlighted {
                        P5_CHARCOAL
                    } else {
                        P5_OFF_BLACK
                    };
                    let border_color = if slot.is_highlighted {
                        BorderColor::all(P5_RED)
                    } else if slot.is_error {
                        BorderColor::all(P5_BRIGHT_RED)
                    } else {
                        BorderColor::all(P5_BORDER)
                    };
                    let val_color = if slot.is_error {
                        P5_BRIGHT_RED
                    } else {
                        P5_WHITE
                    };

                    row.spawn((
                        Node {
                            flex_direction: FlexDirection::Column,
                            padding: UiRect::axes(Val::Px(6.0), Val::Px(5.0)),
                            border: UiRect::all(Val::Px(1.5)),
                            align_items: AlignItems::Center,
                            width: Val::Px(74.0),
                            row_gap: Val::Px(2.0),
                            ..default()
                        },
                        BackgroundColor(bg_color),
                        border_color,
                    ))
                    .with_children(|cell| {
                        cell.spawn((
                            Text::new(slot.label),
                            TextFont::from_font_size(FONT_TAG_SM).with_font(font.clone()),
                            TextColor(P5_MUTED),
                        ));
                        cell.spawn((
                            Text::new(slot.value),
                            TextFont::from_font_size(FONT_BODY_SM).with_font(font.clone()),
                            TextColor(val_color),
                        ));
                        cell.spawn((
                            Text::new(slot.state),
                            TextFont::from_font_size(FONT_TAG_SM).with_font(font.clone()),
                            TextColor(P5_LIGHT_GREY),
                        ));
                    });
                }
            });

            // Pointer Indicators
            card.spawn(Node {
                flex_direction: FlexDirection::Row,
                column_gap: Val::Px(6.0),
                ..default()
            })
            .with_children(|row| {
                for i in 0..slots.len() {
                    row.spawn(Node {
                        width: Val::Px(74.0),
                        flex_direction: FlexDirection::Column,
                        align_items: AlignItems::Center,
                        ..default()
                    })
                    .with_children(|col| {
                        let is_rd = i == read_idx;
                        let is_wr = i == write_idx;

                        if is_rd || is_wr {
                            col.spawn((
                                Text::new("▲"),
                                TextFont::from_font_size(FONT_TAG_SM).with_font(font.clone()),
                                TextColor(P5_RED),
                            ));

                            let label = match (is_rd, is_wr) {
                                (true, true) => "RD+WR",
                                (true, false) => "READ",
                                (false, true) => "WRITE",
                                (false, false) => "",
                            };

                            col.spawn((
                                Text::new(label),
                                TextFont::from_font_size(FONT_TAG_SM).with_font(font.clone()),
                                TextColor(P5_WHITE),
                            ));
                        }
                    });
                }
            });
        });
}

pub struct HistogramBucket {
    pub label: &'static str,
    pub count: u32,
    pub max_count: u32,
    pub is_outlier: bool,
}

/// Spawns a DTrace-style power-of-two latency quantize histogram with proportional bars.
pub fn spawn_histogram(
    parent: &mut ChildSpawnerCommands,
    font: Handle<Font>,
    title: &str,
    buckets: Vec<HistogramBucket>,
    max_bar_width: f32,
) {
    parent
        .spawn((
            Node {
                flex_direction: FlexDirection::Column,
                padding: UiRect::all(Val::Px(12.0)),
                row_gap: Val::Px(5.0),
                border: UiRect {
                    left: Val::Px(4.0),
                    top: Val::Px(1.5),
                    right: Val::Px(1.5),
                    bottom: Val::Px(1.5),
                },
                ..default()
            },
            BackgroundColor(P5_BLACK),
            BorderColor {
                left: P5_RED,
                top: P5_BORDER,
                right: P5_BORDER,
                bottom: P5_BORDER,
            },
        ))
        .with_children(|card| {
            card.spawn((
                Text::new(title),
                TextFont::from_font_size(FONT_BODY_LG).with_font(font.clone()),
                TextColor(P5_WHITE),
            ));

            for bucket in buckets {
                card.spawn(Node {
                    flex_direction: FlexDirection::Row,
                    align_items: AlignItems::Center,
                    column_gap: Val::Px(6.0),
                    ..default()
                })
                .with_children(|row| {
                    let label_color = if bucket.is_outlier { P5_RED } else { P5_MUTED };
                    let bar_color = if bucket.is_outlier {
                        P5_BRIGHT_RED
                    } else {
                        P5_RED
                    };

                    // Label (timestamp bucket in ns)
                    row.spawn(Node {
                        width: Val::Px(68.0),
                        justify_content: JustifyContent::FlexEnd,
                        ..default()
                    })
                    .with_children(|lbl| {
                        lbl.spawn((
                            Text::new(bucket.label),
                            TextFont::from_font_size(FONT_CODE).with_font(font.clone()),
                            TextColor(label_color),
                        ));
                    });

                    // Bar
                    let fraction = if bucket.max_count > 0 {
                        (bucket.count as f32 / bucket.max_count as f32).clamp(0.0, 1.0)
                    } else {
                        0.0
                    };
                    let bar_width = if bucket.count > 0 {
                        (fraction * max_bar_width).max(4.0)
                    } else {
                        0.0
                    };

                    row.spawn((
                        Node {
                            width: Val::Px(bar_width),
                            height: Val::Px(12.0),
                            ..default()
                        },
                        BackgroundColor(bar_color),
                    ));

                    // Count text
                    row.spawn((
                        Text::new(bucket.count.to_string()),
                        TextFont::from_font_size(FONT_CODE).with_font(font.clone()),
                        TextColor(if bucket.is_outlier {
                            P5_WHITE
                        } else {
                            P5_LIGHT_GREY
                        }),
                    ));
                });
            }
        });
}
