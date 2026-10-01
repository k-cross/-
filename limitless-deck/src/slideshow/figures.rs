use super::FontAssets;
use crate::theme::colors::*;
use crate::theme::typography::*;
use bevy::prelude::*;

const SLOT_GAP: f32 = 6.0;
const BIT_CELL: f32 = 28.0;
const BIT_GAP: f32 = 4.0;
const BIT_LABEL_WIDTH: f32 = 112.0;
const LANE_NAME_WIDTH: f32 = 112.0;
const PIPE_TITLE_WIDTH: f32 = 150.0;
const PIPE_CELL_WIDTH: f32 = 118.0;
const CORE_WIDTH: f32 = 64.0;
const CORE_GAP: f32 = 8.0;

fn spawn_text(
    parent: &mut ChildSpawnerCommands,
    font: &Handle<Font>,
    text: &str,
    size: f32,
    color: Color,
) {
    parent.spawn((
        Text::new(text),
        TextFont::from_font_size(size).with_font(font.clone()),
        TextColor(color),
    ));
}

fn spawn_panel(
    parent: &mut ChildSpawnerCommands,
    font: &Handle<Font>,
    title: &str,
    accent: Color,
    align_items: AlignItems,
    build: impl FnOnce(&mut ChildSpawnerCommands),
) {
    parent
        .spawn((
            Node {
                flex_direction: FlexDirection::Column,
                align_items,
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
                left: accent,
                top: P5_BORDER,
                right: P5_BORDER,
                bottom: P5_BORDER,
            },
        ))
        .with_children(|card| {
            if !title.is_empty() {
                spawn_text(card, font, title, FONT_LABEL_SM, accent);
            }
            build(card);
        });
}

fn spawn_chip(
    parent: &mut ChildSpawnerCommands,
    font: &Handle<Font>,
    text: &str,
    text_color: Color,
    background: Color,
    border: Color,
) {
    parent
        .spawn((
            Node {
                padding: UiRect::axes(Val::Px(7.0), Val::Px(4.0)),
                border: UiRect::all(Val::Px(1.5)),
                ..default()
            },
            BackgroundColor(background),
            BorderColor::all(border),
        ))
        .with_children(|chip| {
            spawn_text(chip, font, text, FONT_LABEL_SM, text_color);
        });
}

pub struct Lane {
    pub name: &'static str,
    pub color: Color,
    pub steps: Vec<&'static str>,
    pub highlights: Vec<usize>,
}

pub fn spawn_thread_lanes(
    parent: &mut ChildSpawnerCommands,
    fonts: &FontAssets,
    title: &str,
    lanes: Vec<Lane>,
) {
    spawn_panel(
        parent,
        &fonts.sans,
        title,
        P5_RED,
        AlignItems::FlexStart,
        |card| {
            for lane in lanes {
                card.spawn(Node {
                    flex_direction: FlexDirection::Row,
                    align_items: AlignItems::Center,
                    column_gap: Val::Px(5.0),
                    ..default()
                })
                .with_children(|row| {
                    row.spawn(Node {
                        width: Val::Px(LANE_NAME_WIDTH),
                        ..default()
                    })
                    .with_children(|name| {
                        spawn_text(name, &fonts.display, lane.name, FONT_LABEL, lane.color);
                    });

                    for (i, step) in lane.steps.iter().enumerate() {
                        if lane.highlights.contains(&i) {
                            spawn_chip(row, &fonts.sans, step, P5_WHITE, P5_RED, P5_WHITE);
                        } else {
                            spawn_chip(
                                row,
                                &fonts.sans,
                                step,
                                P5_OFF_WHITE,
                                P5_CHARCOAL,
                                P5_BORDER,
                            );
                        }
                        if i + 1 < lane.steps.len() {
                            spawn_text(row, &fonts.symbols, "→", FONT_LABEL, P5_MUTED);
                        }
                    }
                });
            }
        },
    );
}

#[derive(Clone, Copy)]
pub enum SlotTone {
    Normal,
    Active,
    Danger,
}

pub struct SceneSlot {
    pub label: &'static str,
    pub fields: Vec<&'static str>,
    pub tone: SlotTone,
}

pub struct SceneMarker {
    pub label: &'static str,
    pub slot: usize,
    pub color: Color,
}

pub struct SlotScene {
    pub title: &'static str,
    pub slots: Vec<SceneSlot>,
    pub cursors: Vec<SceneMarker>,
    pub actors: Vec<SceneMarker>,
}

fn spawn_marker_row(
    parent: &mut ChildSpawnerCommands,
    fonts: &FontAssets,
    markers: &[SceneMarker],
    columns: usize,
    slot_width: f32,
    pointing_up: bool,
) {
    parent
        .spawn(Node {
            flex_direction: FlexDirection::Row,
            column_gap: Val::Px(SLOT_GAP),
            ..default()
        })
        .with_children(|row| {
            for column in 0..columns {
                row.spawn(Node {
                    width: Val::Px(slot_width),
                    min_height: Val::Px(30.0),
                    flex_direction: FlexDirection::Column,
                    align_items: AlignItems::Center,
                    justify_content: if pointing_up {
                        JustifyContent::FlexStart
                    } else {
                        JustifyContent::FlexEnd
                    },
                    ..default()
                })
                .with_children(|slot_column| {
                    for marker in markers.iter().filter(|marker| marker.slot == column) {
                        let text = if pointing_up {
                            format!("▲ {}", marker.label)
                        } else {
                            format!("{} ▼", marker.label)
                        };
                        slot_column.spawn((
                            Text::new(text),
                            TextFont::from_font_size(FONT_LABEL_SM).with_font(fonts.sans.clone()),
                            TextColor(marker.color),
                        ));
                    }
                });
            }
        });
}

pub fn spawn_slot_scene(
    parent: &mut ChildSpawnerCommands,
    fonts: &FontAssets,
    scene: SlotScene,
    slot_width: f32,
) {
    spawn_panel(
        parent,
        &fonts.sans,
        scene.title,
        P5_RED,
        AlignItems::Center,
        |card| {
            spawn_marker_row(
                card,
                fonts,
                &scene.cursors,
                scene.slots.len(),
                slot_width,
                false,
            );

            card.spawn(Node {
                flex_direction: FlexDirection::Row,
                column_gap: Val::Px(SLOT_GAP),
                ..default()
            })
            .with_children(|row| {
                for slot in &scene.slots {
                    let (background, border, value_color) = match slot.tone {
                        SlotTone::Normal => (P5_OFF_BLACK, P5_BORDER, P5_WHITE),
                        SlotTone::Active => (P5_CHARCOAL, P5_RED, P5_WHITE),
                        SlotTone::Danger => (P5_DARK_RED, P5_BRIGHT_RED, P5_WHITE),
                    };
                    row.spawn((
                        Node {
                            width: Val::Px(slot_width),
                            flex_direction: FlexDirection::Column,
                            align_items: AlignItems::Center,
                            padding: UiRect::axes(Val::Px(6.0), Val::Px(6.0)),
                            row_gap: Val::Px(3.0),
                            border: UiRect::all(Val::Px(1.5)),
                            ..default()
                        },
                        BackgroundColor(background),
                        BorderColor::all(border),
                    ))
                    .with_children(|cell| {
                        spawn_text(cell, &fonts.sans_heavy, slot.label, FONT_TAG_SM, P5_MUTED);
                        for field in &slot.fields {
                            spawn_text(cell, &fonts.monospace, field, FONT_LABEL_SM, value_color);
                        }
                    });
                }
            });

            spawn_marker_row(
                card,
                fonts,
                &scene.actors,
                scene.slots.len(),
                slot_width,
                true,
            );
        },
    );
}

pub struct BitRow {
    pub caption: Option<&'static str>,
    pub label: &'static str,
    pub bits: &'static str,
    pub note: &'static str,
}

pub fn spawn_bit_rows(
    parent: &mut ChildSpawnerCommands,
    fonts: &FontAssets,
    title: &str,
    rows: Vec<BitRow>,
    mcb_bit: usize,
) {
    let bit_count = rows.first().map_or(0, |row| row.bits.len());
    spawn_panel(
        parent,
        &fonts.sans,
        title,
        P5_RED,
        AlignItems::FlexStart,
        |card| {
            card.spawn(Node {
                flex_direction: FlexDirection::Row,
                column_gap: Val::Px(BIT_GAP),
                padding: UiRect::left(Val::Px(BIT_LABEL_WIDTH + BIT_GAP)),
                ..default()
            })
            .with_children(|header| {
                for position in 0..bit_count {
                    let bit_index = bit_count - 1 - position;
                    let color = if bit_index == mcb_bit {
                        P5_GOLD
                    } else {
                        P5_MUTED
                    };
                    header
                        .spawn(Node {
                            width: Val::Px(BIT_CELL),
                            justify_content: JustifyContent::Center,
                            ..default()
                        })
                        .with_children(|slot| {
                            spawn_text(
                                slot,
                                &fonts.monospace,
                                &bit_index.to_string(),
                                FONT_TAG_SM,
                                color,
                            );
                        });
                }
            });

            for row in rows {
                if let Some(caption) = row.caption {
                    card.spawn(Node {
                        padding: UiRect::left(Val::Px(BIT_LABEL_WIDTH + BIT_GAP)),
                        ..default()
                    })
                    .with_children(|line| {
                        spawn_text(
                            line,
                            &fonts.sans_heavy,
                            &format!("▼ {caption}"),
                            FONT_TAG_SM,
                            P5_GOLD,
                        );
                    });
                }

                card.spawn(Node {
                    flex_direction: FlexDirection::Row,
                    align_items: AlignItems::Center,
                    column_gap: Val::Px(BIT_GAP),
                    ..default()
                })
                .with_children(|line| {
                    line.spawn(Node {
                        width: Val::Px(BIT_LABEL_WIDTH),
                        ..default()
                    })
                    .with_children(|label| {
                        spawn_text(label, &fonts.display, row.label, FONT_LABEL, P5_WHITE);
                    });

                    for (position, bit) in row.bits.chars().enumerate() {
                        let bit_index = bit_count - 1 - position;
                        let is_set = bit == '1';
                        let border = if bit_index == mcb_bit {
                            P5_GOLD
                        } else if is_set {
                            P5_WHITE
                        } else {
                            P5_BORDER
                        };
                        line.spawn((
                            Node {
                                width: Val::Px(BIT_CELL),
                                height: Val::Px(BIT_CELL),
                                justify_content: JustifyContent::Center,
                                align_items: AlignItems::Center,
                                border: UiRect::all(Val::Px(1.5)),
                                ..default()
                            },
                            BackgroundColor(if is_set { P5_RED } else { P5_CHARCOAL }),
                            BorderColor::all(border),
                        ))
                        .with_children(|cell| {
                            spawn_text(
                                cell,
                                &fonts.monospace,
                                &bit.to_string(),
                                FONT_BODY,
                                if is_set { P5_WHITE } else { P5_MUTED },
                            );
                        });
                    }

                    line.spawn(Node {
                        margin: UiRect::left(Val::Px(10.0)),
                        ..default()
                    })
                    .with_children(|note| {
                        spawn_text(note, &fonts.monospace, row.note, FONT_LABEL, P5_LIGHT_GREY);
                    });
                });
            }
        },
    );
}

pub enum PipeCell {
    Instr(&'static str),
    Wrong(&'static str),
    Branch(&'static str),
    Gap,
}

pub struct PipePhase {
    pub title: &'static str,
    pub cells: Vec<PipeCell>,
}

pub fn spawn_pipeline_grid(
    parent: &mut ChildSpawnerCommands,
    fonts: &FontAssets,
    title: &str,
    stages: &[&str],
    phases: Vec<PipePhase>,
) {
    spawn_panel(
        parent,
        &fonts.sans,
        title,
        P5_RED,
        AlignItems::FlexStart,
        |card| {
            card.spawn(Node {
                flex_direction: FlexDirection::Row,
                column_gap: Val::Px(SLOT_GAP),
                padding: UiRect::left(Val::Px(PIPE_TITLE_WIDTH + SLOT_GAP)),
                ..default()
            })
            .with_children(|header| {
                for stage in stages {
                    header
                        .spawn(Node {
                            width: Val::Px(PIPE_CELL_WIDTH),
                            justify_content: JustifyContent::Center,
                            ..default()
                        })
                        .with_children(|cell| {
                            spawn_text(cell, &fonts.display, stage, FONT_LABEL, P5_GOLD);
                        });
                }
            });

            for phase in phases {
                card.spawn(Node {
                    flex_direction: FlexDirection::Row,
                    align_items: AlignItems::Center,
                    column_gap: Val::Px(SLOT_GAP),
                    ..default()
                })
                .with_children(|row| {
                    row.spawn(Node {
                        width: Val::Px(PIPE_TITLE_WIDTH),
                        ..default()
                    })
                    .with_children(|title| {
                        spawn_text(title, &fonts.display, phase.title, FONT_LABEL, P5_WHITE);
                    });

                    for cell in &phase.cells {
                        let (text, text_color, background, border) = match cell {
                            PipeCell::Instr(text) => (*text, P5_OFF_WHITE, P5_CHARCOAL, P5_BORDER),
                            PipeCell::Wrong(text) => (*text, P5_WHITE, P5_DARK_RED, P5_BRIGHT_RED),
                            PipeCell::Branch(text) => (*text, P5_WHITE, P5_RED, P5_WHITE),
                            PipeCell::Gap => ("GAP", P5_MUTED, Color::NONE, P5_MUTED),
                        };
                        row.spawn((
                            Node {
                                width: Val::Px(PIPE_CELL_WIDTH),
                                height: Val::Px(34.0),
                                justify_content: JustifyContent::Center,
                                align_items: AlignItems::Center,
                                border: UiRect::all(Val::Px(1.5)),
                                ..default()
                            },
                            BackgroundColor(background),
                            BorderColor::all(border),
                        ))
                        .with_children(|cell_node| {
                            spawn_text(cell_node, &fonts.sans, text, FONT_LABEL_SM, text_color);
                        });
                    }
                });
            }

            card.spawn(Node {
                flex_direction: FlexDirection::Row,
                column_gap: Val::Px(8.0),
                padding: UiRect::left(Val::Px(PIPE_TITLE_WIDTH + SLOT_GAP)),
                ..default()
            })
            .with_children(|legend| {
                spawn_chip(legend, &fonts.sans, "BRANCH", P5_WHITE, P5_RED, P5_WHITE);
                spawn_chip(
                    legend,
                    &fonts.sans,
                    "WRONG PATH",
                    P5_WHITE,
                    P5_DARK_RED,
                    P5_BRIGHT_RED,
                );
                spawn_chip(
                    legend,
                    &fonts.sans,
                    "BUBBLE",
                    P5_MUTED,
                    Color::NONE,
                    P5_MUTED,
                );
            });
        },
    );
}

pub fn spawn_core_cluster(
    parent: &mut ChildSpawnerCommands,
    fonts: &FontAssets,
    title: &str,
    accent: Color,
    cores: &[&str],
    target: &str,
) {
    let row_width = cores.len() as f32 * CORE_WIDTH + (cores.len() as f32 - 1.0) * CORE_GAP;
    spawn_panel(
        parent,
        &fonts.sans,
        title,
        accent,
        AlignItems::Center,
        |card| {
            card.spawn(Node {
                flex_direction: FlexDirection::Row,
                column_gap: Val::Px(CORE_GAP),
                ..default()
            })
            .with_children(|row| {
                for core in cores {
                    row.spawn((
                        Node {
                            width: Val::Px(CORE_WIDTH),
                            padding: UiRect::axes(Val::Px(4.0), Val::Px(6.0)),
                            justify_content: JustifyContent::Center,
                            border: UiRect::all(Val::Px(1.5)),
                            ..default()
                        },
                        BackgroundColor(P5_CHARCOAL),
                        BorderColor::all(P5_BORDER),
                    ))
                    .with_children(|cell| {
                        spawn_text(cell, &fonts.sans_heavy, core, FONT_LABEL_SM, P5_OFF_WHITE);
                    });
                }
            });

            card.spawn(Node {
                flex_direction: FlexDirection::Row,
                column_gap: Val::Px(CORE_GAP),
                ..default()
            })
            .with_children(|row| {
                for _ in cores {
                    row.spawn(Node {
                        width: Val::Px(CORE_WIDTH),
                        justify_content: JustifyContent::Center,
                        ..default()
                    })
                    .with_children(|arrow| {
                        spawn_text(arrow, &fonts.symbols, "▼", FONT_BODY, accent);
                    });
                }
            });

            card.spawn((
                Node {
                    width: Val::Px(row_width),
                    padding: UiRect::axes(Val::Px(8.0), Val::Px(8.0)),
                    justify_content: JustifyContent::Center,
                    border: UiRect::all(Val::Px(2.0)),
                    ..default()
                },
                BackgroundColor(P5_DARK_RED),
                BorderColor::all(accent),
            ))
            .with_children(|index| {
                spawn_text(index, &fonts.display, target, FONT_BODY, P5_WHITE);
            });
        },
    );
}
