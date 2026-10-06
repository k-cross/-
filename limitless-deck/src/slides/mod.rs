pub mod auto_slides;
pub mod figure_slides;
pub mod intro;

use crate::slideshow::SlideState;
use bevy::prelude::*;

pub struct SlidesPlugin;

impl Plugin for SlidesPlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<intro::IntroSelection>()
            .add_systems(OnEnter(SlideState::Intro), intro::spawn_intro_slide)
            .add_systems(
                Update,
                (
                    intro::handle_intro_menu_input,
                    intro::update_intro_menu_visuals
                        .run_if(resource_changed::<intro::IntroSelection>),
                    intro::animate_intro_menu_cards,
                )
                    .run_if(in_state(SlideState::Intro)),
            )
            .add_systems(
                OnEnter(SlideState::MutexBottleneck),
                auto_slides::spawn_sync_atomics_slide,
            )
            .add_systems(
                OnEnter(SlideState::SizeFocus),
                auto_slides::spawn_size_focus_slide,
            )
            .add_systems(
                OnEnter(SlideState::StateAmbiguity),
                auto_slides::spawn_state_ambiguity_slide,
            )
            .add_systems(
                OnEnter(SlideState::BoolRep),
                auto_slides::spawn_bool_rep_slide,
            )
            .add_systems(
                OnEnter(SlideState::ThreadLanes),
                figure_slides::spawn_thread_lanes_slide,
            )
            .add_systems(
                OnEnter(SlideState::AbaScenario),
                figure_slides::spawn_aba_scenario_slide,
            )
            .add_systems(
                OnEnter(SlideState::MemInit),
                auto_slides::spawn_mem_init_slide,
            )
            .add_systems(
                OnEnter(SlideState::AbaProblem),
                auto_slides::spawn_aba_problem_slide,
            )
            .add_systems(
                OnEnter(SlideState::ThreadSafety),
                auto_slides::spawn_thread_safety_slide,
            )
            .add_systems(
                OnEnter(SlideState::Recovery),
                auto_slides::spawn_recovery_slide,
            )
            .add_systems(
                OnEnter(SlideState::AutomateRecovery),
                auto_slides::spawn_automate_recovery_slide,
            )
            .add_systems(
                OnEnter(SlideState::CacheContention),
                auto_slides::spawn_cache_contention_slide,
            )
            .add_systems(
                OnEnter(SlideState::DisassembledCode),
                auto_slides::spawn_disassembled_code_slide,
            )
            .add_systems(
                OnEnter(SlideState::BitWalk),
                figure_slides::spawn_bit_walk_slide,
            )
            .add_systems(
                OnEnter(SlideState::BranchlessIndex),
                auto_slides::spawn_branchless_index_slide,
            )
            .add_systems(
                OnEnter(SlideState::PipelineFlush),
                figure_slides::spawn_pipeline_flush_slide,
            )
            .add_systems(
                OnEnter(SlideState::TailLatency),
                auto_slides::spawn_tail_latency_slide,
            )
            .add_systems(
                OnEnter(SlideState::UnboundedLatency),
                auto_slides::spawn_unbounded_latency_slide,
            )
            .add_systems(
                OnEnter(SlideState::Backoffs),
                auto_slides::spawn_backoffs_slide,
            )
            .add_systems(
                OnEnter(SlideState::CoreTopology),
                figure_slides::spawn_core_topology_slide,
            )
            .add_systems(
                OnEnter(SlideState::Scaling),
                auto_slides::spawn_scaling_slide,
            )
            .add_systems(
                OnEnter(SlideState::Simplification),
                auto_slides::spawn_simplification_slide,
            );
    }
}
