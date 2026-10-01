use super::auto_slides::{CalloutCard, spawn_callout_card, spawn_slide_frame};
use crate::slideshow::FontAssets;
use crate::slideshow::SlideState;
use crate::slideshow::animation::SlamEntrance;
use crate::slideshow::figures::*;
use crate::theme::colors::*;
use bevy::prelude::*;

fn spawn_figure_body(
    slide: &mut ChildSpawnerCommands,
    build: impl FnOnce(&mut ChildSpawnerCommands),
) {
    slide
        .spawn((
            Node {
                width: Val::Percent(100.0),
                flex_direction: FlexDirection::Column,
                align_items: AlignItems::Center,
                row_gap: Val::Px(14.0),
                ..default()
            },
            SlamEntrance::new(Vec2::new(0.0, 80.0), -0.04, 0.08),
        ))
        .with_children(build);
}

pub fn spawn_thread_lanes_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    spawn_slide_frame(
        &mut commands,
        &font_assets,
        SlideState::ThreadLanes,
        "/// FIGURE // THREAD LANES ///",
        "WHO CHECKS WHAT, AND WHEN",
        |slide, fonts| {
            spawn_figure_body(slide, |body| {
                spawn_thread_lanes(
                    body,
                    fonts,
                    "/// FLAG CHECK AFTER THE INDEX READ ///",
                    vec![
                        Lane {
                            name: "WRITE THREAD",
                            color: P5_RED,
                            steps: vec![
                                "Check Full",
                                "Get Index",
                                "Proceed if Uninitialized",
                                "Update Index",
                                "Write Value",
                                "Initialized = true",
                            ],
                            highlights: vec![2],
                        },
                        Lane {
                            name: "READ THREAD 1",
                            color: P5_WHITE,
                            steps: vec![
                                "Check Empty",
                                "Get Index",
                                "Proceed if Initialized",
                                "Get Value",
                                "Update Index",
                                "Uninitialize",
                            ],
                            highlights: vec![2],
                        },
                        Lane {
                            name: "READ THREAD 2",
                            color: P5_WHITE,
                            steps: vec![
                                "Check Empty",
                                "Get Index",
                                "Proceed if Initialized",
                                "Get Value",
                                "Update Index",
                                "Uninitialize",
                            ],
                            highlights: vec![2],
                        },
                    ],
                );
                spawn_thread_lanes(
                    body,
                    fonts,
                    "/// SIZE CHECKS MOVED BELOW THE FLAG CHECK ///",
                    vec![
                        Lane {
                            name: "WRITE THREAD",
                            color: P5_RED,
                            steps: vec![
                                "Get Index",
                                "Proceed if Uninitialized",
                                "Check Full",
                                "Update Index",
                                "Write Value",
                                "Initialized = true",
                            ],
                            highlights: vec![1, 2],
                        },
                        Lane {
                            name: "READ THREAD",
                            color: P5_WHITE,
                            steps: vec![
                                "Get Index",
                                "Check Empty",
                                "Proceed if Initialized",
                                "Get Value",
                                "Update Index",
                                "Uninitialize",
                            ],
                            highlights: vec![1, 2],
                        },
                    ],
                );
                spawn_callout_card(
                    body,
                    fonts,
                    CalloutCard {
                        icon: "!",
                        title: "THE WINDOW BETWEEN CHECK AND COMMIT",
                        text: "Reordering the checks still leaves a gap: a reader can pass 'Proceed if Initialized' and then be descheduled before it updates the index.",
                        is_alert: true,
                    },
                );
            });
        },
    );
}

pub fn spawn_aba_scenario_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    spawn_slide_frame(
        &mut commands,
        &font_assets,
        SlideState::AbaScenario,
        "/// FIGURE // ABA WITH A BOOLEAN FLAG ///",
        "THE SLEEPING READER",
        |slide, fonts| {
            spawn_figure_body(slide, |body| {
                body.spawn(Node {
                    flex_direction: FlexDirection::Row,
                    align_items: AlignItems::Center,
                    column_gap: Val::Px(10.0),
                    ..default()
                })
                .with_children(|row| {
                    spawn_slot_scene(
                        row,
                        fonts,
                        SlotScene {
                            title: "/// BEFORE: THREAD 1 SLEEPS AFTER ITS FLAG CHECK ///",
                            slots: vec![
                                SceneSlot {
                                    label: "SLOT 1",
                                    fields: vec!["init: true", "val: 1"],
                                    tone: SlotTone::Active,
                                },
                                SceneSlot {
                                    label: "SLOT 2",
                                    fields: vec!["init: true", "val: 2"],
                                    tone: SlotTone::Normal,
                                },
                                SceneSlot {
                                    label: "SLOT 3",
                                    fields: vec!["init: true", "val: 3"],
                                    tone: SlotTone::Normal,
                                },
                                SceneSlot {
                                    label: "SLOT 4",
                                    fields: vec!["init: false", "val: 0x0"],
                                    tone: SlotTone::Normal,
                                },
                            ],
                            cursors: vec![
                                SceneMarker {
                                    label: "READ",
                                    slot: 0,
                                    color: P5_WHITE,
                                },
                                SceneMarker {
                                    label: "WRITE",
                                    slot: 3,
                                    color: P5_RED,
                                },
                            ],
                            actors: vec![SceneMarker {
                                label: "T1 sleeps @ CAS",
                                slot: 0,
                                color: P5_GOLD,
                            }],
                        },
                        112.0,
                    );

                    row.spawn(Node {
                        width: Val::Px(92.0),
                        flex_direction: FlexDirection::Column,
                        align_items: AlignItems::Center,
                        row_gap: Val::Px(4.0),
                        ..default()
                    })
                    .with_children(|middle| {
                        middle.spawn((
                            Text::new("▶"),
                            TextFont::from_font_size(28.0).with_font(fonts.symbols.clone()),
                            TextColor(P5_RED),
                        ));
                        middle.spawn((
                            Text::new("T2…Tn lap the whole buffer"),
                            TextFont::from_font_size(10.5).with_font(fonts.sans.clone()),
                            TextColor(P5_LIGHT_GREY),
                            TextLayout::justify(Justify::Center),
                        ));
                    });

                    spawn_slot_scene(
                        row,
                        fonts,
                        SlotScene {
                            title: "/// AFTER: EVERY SLOT IS FALSE AGAIN ///",
                            slots: vec![
                                SceneSlot {
                                    label: "SLOT 1",
                                    fields: vec!["init: false", "val: 1"],
                                    tone: SlotTone::Danger,
                                },
                                SceneSlot {
                                    label: "SLOT 2",
                                    fields: vec!["init: false", "val: 2"],
                                    tone: SlotTone::Normal,
                                },
                                SceneSlot {
                                    label: "SLOT 3",
                                    fields: vec!["init: false", "val: 3"],
                                    tone: SlotTone::Normal,
                                },
                                SceneSlot {
                                    label: "SLOT 4",
                                    fields: vec!["init: false", "val: 4"],
                                    tone: SlotTone::Normal,
                                },
                            ],
                            cursors: vec![
                                SceneMarker {
                                    label: "READ",
                                    slot: 0,
                                    color: P5_WHITE,
                                },
                                SceneMarker {
                                    label: "WRITE",
                                    slot: 0,
                                    color: P5_RED,
                                },
                            ],
                            actors: vec![SceneMarker {
                                label: "T1 wakes @ CAS",
                                slot: 0,
                                color: P5_GOLD,
                            }],
                        },
                        112.0,
                    );
                });
                spawn_callout_card(
                    body,
                    fonts,
                    CalloutCard {
                        icon: "!",
                        title: "SAME INDEX, DIFFERENT WORLD",
                        text: "Thread 1 checked the flag, slept, and woke up when the cursors had lapped back to the same slot. Its CAS still matches, so it can read uninitialized data.",
                        is_alert: true,
                    },
                );
            });
        },
    );
}

pub fn spawn_bit_walk_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    spawn_slide_frame(
        &mut commands,
        &font_assets,
        SlideState::BitWalk,
        "/// FIGURE // STAMP ARITHMETIC ///",
        "THE LAP BIT",
        |slide, fonts| {
            spawn_figure_body(slide, |body| {
                body.spawn(Node {
                    flex_direction: FlexDirection::Row,
                    align_items: AlignItems::FlexStart,
                    column_gap: Val::Px(14.0),
                    ..default()
                })
                .with_children(|row| {
                    spawn_bit_rows(
                        row,
                        fonts,
                        "/// WRAPPING SHIFTS THE FAR-REACHING BIT (CAPACITY 7, MCB 8) ///",
                        vec![
                            BitRow {
                                caption: None,
                                label: "STAMP 6",
                                bits: "000110",
                                note: "7 & 6 = index 6",
                            },
                            BitRow {
                                caption: Some("WRAP: SKIP TO 8"),
                                label: "STAMP 8",
                                bits: "001000",
                                note: "7 & 8 = index 0",
                            },
                            BitRow {
                                caption: Some("NEXT LAP, SAME INDEX"),
                                label: "STAMP 14",
                                bits: "001110",
                                note: "7 & 14 = index 6",
                            },
                            BitRow {
                                caption: Some("WRAP AGAIN: SKIP TO 16"),
                                label: "STAMP 16",
                                bits: "010000",
                                note: "7 & 16 = index 0",
                            },
                        ],
                        3,
                    );
                    spawn_bit_rows(
                        row,
                        fonts,
                        "/// MCB ^ STAMP FLIPS THE LAP BIT ///",
                        vec![
                            BitRow {
                                caption: None,
                                label: "STAMP 14",
                                bits: "001110",
                                note: "lap bit set",
                            },
                            BitRow {
                                caption: Some("XOR MCB (0b001000)"),
                                label: "RESULT 6",
                                bits: "000110",
                                note: "lap bit clear",
                            },
                            BitRow {
                                caption: Some("XOR MCB AGAIN"),
                                label: "RESULT 14",
                                bits: "001110",
                                note: "lap bit set",
                            },
                        ],
                        3,
                    );
                });
                spawn_callout_card(
                    body,
                    fonts,
                    CalloutCard {
                        icon: "★",
                        title: "INDEX AND LAP IN ONE WORD",
                        text: "index = (MCB - 1) & stamp, with MCB = capacity.next_power_of_two(). The bit at MCB never touches the index, so flipping it records that a lap happened.",
                        is_alert: false,
                    },
                );
            });
        },
    );
}

pub fn spawn_pipeline_flush_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    spawn_slide_frame(
        &mut commands,
        &font_assets,
        SlideState::PipelineFlush,
        "/// FIGURE // THE CPU PIPELINE ///",
        "WHEN THE CPU GUESSES WRONG",
        |slide, fonts| {
            spawn_figure_body(slide, |body| {
                spawn_pipeline_grid(
                    body,
                    fonts,
                    "/// MISPREDICTED BRANCH: FLUSH AND REFILL ///",
                    &["IF", "ID", "EX", "MEM", "WB"],
                    vec![
                        PipePhase {
                            title: "MISPREDICTED",
                            cells: vec![
                                PipeCell::Wrong("Instr D"),
                                PipeCell::Wrong("Instr C"),
                                PipeCell::Branch("Branch"),
                                PipeCell::Instr("Instr B"),
                                PipeCell::Instr("Instr A"),
                            ],
                        },
                        PipePhase {
                            title: "DETECTED",
                            cells: vec![
                                PipeCell::Wrong("Instr E"),
                                PipeCell::Wrong("Instr D"),
                                PipeCell::Wrong("Instr C"),
                                PipeCell::Branch("Branch"),
                                PipeCell::Instr("Instr B"),
                            ],
                        },
                        PipePhase {
                            title: "FLUSHED",
                            cells: vec![
                                PipeCell::Instr("Correct Path 1"),
                                PipeCell::Gap,
                                PipeCell::Gap,
                                PipeCell::Gap,
                                PipeCell::Branch("Branch"),
                            ],
                        },
                        PipePhase {
                            title: "REFILL",
                            cells: vec![
                                PipeCell::Instr("Correct Path 2"),
                                PipeCell::Instr("Correct Path 1"),
                                PipeCell::Gap,
                                PipeCell::Gap,
                                PipeCell::Gap,
                            ],
                        },
                    ],
                );
                spawn_callout_card(
                    body,
                    fonts,
                    CalloutCard {
                        icon: "⚡",
                        title: "BUBBLES COST CYCLES",
                        text: "A wrong guess throws away everything fetched behind the branch, and the stages stay empty while the correct path refills. Branchless index math gives the CPU nothing to guess.",
                        is_alert: false,
                    },
                );
            });
        },
    );
}

pub fn spawn_core_topology_slide(mut commands: Commands, font_assets: Res<FontAssets>) {
    spawn_slide_frame(
        &mut commands,
        &font_assets,
        SlideState::CoreTopology,
        "/// FIGURE // CONTENTION TOPOLOGY ///",
        "FOUR CORES, ONE INDEX",
        |slide, fonts| {
            spawn_figure_body(slide, |body| {
                body.spawn(Node {
                    flex_direction: FlexDirection::Row,
                    align_items: AlignItems::FlexStart,
                    column_gap: Val::Px(24.0),
                    ..default()
                })
                .with_children(|row| {
                    spawn_core_cluster(
                        row,
                        fonts,
                        "/// 4 READ THREADS ///",
                        P5_WHITE,
                        &["CORE 1", "CORE 2", "CORE 3", "CORE 4"],
                        "READ INDEX",
                    );
                    spawn_core_cluster(
                        row,
                        fonts,
                        "/// 4 WRITE THREADS ///",
                        P5_RED,
                        &["CORE 5", "CORE 6", "CORE 7", "CORE 8"],
                        "WRITE INDEX",
                    );
                });
                spawn_callout_card(
                    body,
                    fonts,
                    CalloutCard {
                        icon: "!",
                        title: "EVERY CAS LANDS ON THE SAME LINE",
                        text: "Each group of cores hammers one shared index. Past this point, more cores add invalidation traffic and tail latency instead of throughput.",
                        is_alert: true,
                    },
                );
            });
        },
    );
}
