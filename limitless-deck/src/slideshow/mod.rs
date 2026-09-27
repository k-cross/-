pub mod animation;
pub mod background;
pub mod character;
pub mod code_view;
pub mod diagrams;
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
    MemInit,
    AbaProblem,
    ThreadSafety,
    Recovery,
    AutomateRecovery,
    CacheContention,
    DisassembledCode,
    BranchlessIndex,
    TailLatency,
    UnboundedLatency,
    Backoffs,
    Scaling,
    Simplification,
}

impl SlideState {
    pub const TOTAL_COUNT: usize = 18;

    pub fn from_index(index: usize) -> Self {
        match index {
            0 => SlideState::Intro,
            1 => SlideState::MutexBottleneck,
            2 => SlideState::SizeFocus,
            3 => SlideState::StateAmbiguity,
            4 => SlideState::BoolRep,
            5 => SlideState::MemInit,
            6 => SlideState::AbaProblem,
            7 => SlideState::ThreadSafety,
            8 => SlideState::Recovery,
            9 => SlideState::AutomateRecovery,
            10 => SlideState::CacheContention,
            11 => SlideState::DisassembledCode,
            12 => SlideState::BranchlessIndex,
            13 => SlideState::TailLatency,
            14 => SlideState::UnboundedLatency,
            15 => SlideState::Backoffs,
            16 => SlideState::Scaling,
            17 => SlideState::Simplification,
            _ => SlideState::Intro,
        }
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
                    hud::update_hud.run_if(resource_changed::<SlideController>),
                    hud::animate_hud,
                    code_view::scroll_code_blocks,
                ),
            );
    }
}

fn handle_slide_input(
    mut commands: Commands,
    keys: Res<ButtonInput<KeyCode>>,
    mut controller: ResMut<SlideController>,
    mut next_state: ResMut<NextState<SlideState>>,
    mut windows: Query<&mut Window>,
    character_roots: Query<&mut character::PhantomThiefRoot>,
) {
    let mut changed = false;

    // Next slide actions
    if keys.just_pressed(KeyCode::ArrowRight)
        || keys.just_pressed(KeyCode::Space)
        || keys.just_pressed(KeyCode::Enter)
        || keys.just_pressed(KeyCode::PageDown)
    {
        if controller.current_index + 1 < controller.total_slides {
            controller.current_index += 1;
            changed = true;
        }
    }

    // Previous slide actions
    if keys.just_pressed(KeyCode::ArrowLeft)
        || keys.just_pressed(KeyCode::Backspace)
        || keys.just_pressed(KeyCode::PageUp)
    {
        if controller.current_index > 0 {
            controller.current_index -= 1;
            changed = true;
        }
    }

    // Fullscreen toggle
    if keys.just_pressed(KeyCode::KeyF) || keys.just_pressed(KeyCode::F11) {
        if let Ok(mut window) = windows.single_mut() {
            window.mode = match window.mode {
                bevy::window::WindowMode::Windowed => {
                    bevy::window::WindowMode::BorderlessFullscreen(MonitorSelection::Current)
                }
                _ => bevy::window::WindowMode::Windowed,
            };
        }
    }

    if changed {
        next_state.set(SlideState::from_index(controller.current_index));
        // Spawn razor-sharp screen transition slash
        animation::spawn_screen_slash(&mut commands);
        // Trigger character forward slash/lunge performance
        character::trigger_character_slash(character_roots);
    }
}
