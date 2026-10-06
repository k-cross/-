pub mod animation;
pub mod background;
pub mod character;
pub mod code_view;
pub mod diagrams;
pub mod figures;
pub mod fonts;
pub mod hud;
pub mod splatter;

pub use fonts::FontAssets;

use bevy::prelude::*;

/// Global Slide enumeration
#[derive(States, Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SlideState {
    #[default]
    Intro,
    MutexBottleneck,
    SizeFocus,
    StateAmbiguity,
    BoolRep,
    ThreadLanes,
    AbaScenario,
    MemInit,
    AbaProblem,
    ThreadSafety,
    Recovery,
    AutomateRecovery,
    CacheContention,
    DisassembledCode,
    BitWalk,
    BranchlessIndex,
    PipelineFlush,
    TailLatency,
    UnboundedLatency,
    Backoffs,
    CoreTopology,
    Scaling,
    Simplification,
}

impl SlideState {
    pub const ORDER: &'static [SlideState] = &[
        SlideState::Intro,
        SlideState::MutexBottleneck,
        SlideState::SizeFocus,
        SlideState::StateAmbiguity,
        SlideState::BoolRep,
        SlideState::ThreadLanes,
        SlideState::AbaScenario,
        SlideState::MemInit,
        SlideState::AbaProblem,
        SlideState::ThreadSafety,
        SlideState::Recovery,
        SlideState::AutomateRecovery,
        SlideState::CacheContention,
        SlideState::DisassembledCode,
        SlideState::BitWalk,
        SlideState::BranchlessIndex,
        SlideState::PipelineFlush,
        SlideState::TailLatency,
        SlideState::UnboundedLatency,
        SlideState::Backoffs,
        SlideState::CoreTopology,
        SlideState::Scaling,
        SlideState::Simplification,
    ];

    pub const TOTAL_COUNT: usize = Self::ORDER.len();

    pub fn from_index(index: usize) -> Self {
        Self::ORDER.get(index).copied().unwrap_or_default()
    }
}

/// Resource controlling the active slide and total slide count
#[derive(Resource)]
pub struct SlideController {
    pub current_index: usize,
    pub total_slides: usize,
}

impl Default for SlideController {
    fn default() -> Self {
        Self {
            current_index: 0,
            total_slides: SlideState::TOTAL_COUNT,
        }
    }
}

pub struct SlideshowPlugin;

impl Plugin for SlideshowPlugin {
    fn build(&self, app: &mut App) {
        app.init_state::<SlideState>()
            .init_resource::<SlideController>()
            .add_plugins((
                fonts::FontPlugin,
                background::BackgroundPlugin,
                animation::AnimationPlugin,
                character::CharacterPlugin,
            ))
            .add_systems(Startup, hud::setup_hud)
            .add_systems(
                Update,
                (
                    handle_slide_input,
                    update_ui_scale_on_fullscreen,
                    hud::update_hud.run_if(resource_changed::<SlideController>),
                    code_view::scroll_code_blocks
                        .run_if(any_with_component::<code_view::CodeBlockScroll>),
                ),
            );
    }
}

fn handle_slide_input(
    mut commands: Commands,
    keys: Res<ButtonInput<KeyCode>>,
    mut controller: ResMut<SlideController>,
    current_state: Res<State<SlideState>>,
    mut next_state: ResMut<NextState<SlideState>>,
    mut windows: Query<&mut Window, With<bevy::window::PrimaryWindow>>,
    character_roots: Query<&mut character::PhantomThiefRoot>,
) {
    let mut changed = false;
    let is_intro = *current_state.get() == SlideState::Intro;

    // Next slide actions
    if (keys.just_pressed(KeyCode::ArrowRight)
        || keys.just_pressed(KeyCode::Space)
        || (!is_intro && keys.just_pressed(KeyCode::Enter))
        || keys.just_pressed(KeyCode::PageDown))
        && controller.total_slides > 0
    {
        controller.current_index = (controller.current_index + 1) % controller.total_slides;
        changed = true;
    }

    // Previous slide actions
    if (keys.just_pressed(KeyCode::ArrowLeft)
        || keys.just_pressed(KeyCode::Backspace)
        || keys.just_pressed(KeyCode::PageUp))
        && controller.total_slides > 0
    {
        controller.current_index =
            (controller.current_index + controller.total_slides - 1) % controller.total_slides;
        changed = true;
    }

    // Fullscreen toggle
    if (keys.just_pressed(KeyCode::KeyF) || keys.just_pressed(KeyCode::F11))
        && let Ok(mut window) = windows.single_mut()
    {
        window.mode = match window.mode {
            bevy::window::WindowMode::Windowed => {
                bevy::window::WindowMode::BorderlessFullscreen(MonitorSelection::Current)
            }
            _ => bevy::window::WindowMode::Windowed,
        };
    }

    if changed {
        next_state.set(SlideState::from_index(controller.current_index));
        // Spawn razor-sharp screen transition slash
        animation::spawn_screen_slash(&mut commands);
        // Trigger character forward slash/lunge performance
        character::trigger_character_slash(character_roots);
    }
}

/// Dynamically scales all UI elements when entering fullscreen mode so that
/// text and boxes appear proportionally larger on higher-resolution displays.
/// The design resolution is 1280×720; in fullscreen the UI scales up to
/// maintain visual prominence across the larger viewport.
fn update_ui_scale_on_fullscreen(
    windows: Query<&Window, With<bevy::window::PrimaryWindow>>,
    mut ui_scale: ResMut<UiScale>,
) {
    const BASE_WIDTH: f32 = 1280.0;
    const BASE_HEIGHT: f32 = 720.0;

    if let Ok(window) = windows.single() {
        let new_scale = match window.mode {
            bevy::window::WindowMode::Windowed => 1.0,
            _ => {
                // Scale proportionally to the fullscreen resolution,
                // using the smaller axis ratio to avoid overflow
                let scale_x = window.resolution.width() / BASE_WIDTH;
                let scale_y = window.resolution.height() / BASE_HEIGHT;
                scale_x.min(scale_y).max(1.0)
            }
        };

        // Only update the resource when the scale actually changed to avoid
        // unnecessary change-detection triggers
        if (ui_scale.0 - new_scale).abs() > 0.001 {
            ui_scale.0 = new_scale;
        }
    }
}
