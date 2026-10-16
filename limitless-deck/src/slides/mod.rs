pub mod auto_slides;
pub mod figure_slides;
pub mod intro;

use crate::slideshow::SlideState;
use bevy::ecs::schedule::ScheduleConfigs;
use bevy::ecs::system::ScheduleSystem;
use bevy::prelude::*;

pub struct SlidesPlugin;

impl Plugin for SlidesPlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<intro::IntroSelection>().add_systems(
            Update,
            (
                intro::handle_intro_menu_input,
                intro::update_intro_menu_visuals.run_if(resource_changed::<intro::IntroSelection>),
                intro::animate_intro_menu_cards,
            )
                .run_if(in_state(SlideState::Intro)),
        );

        for &slide in SlideState::ORDER {
            app.add_systems(OnEnter(slide), spawn_systems(slide));
        }
    }
}

fn spawn_systems(slide: SlideState) -> ScheduleConfigs<ScheduleSystem> {
    match slide {
        SlideState::Intro => intro::spawn_intro_slide.into_configs(),
        SlideState::MutexBottleneck => auto_slides::spawn_sync_atomics_slide.into_configs(),
        SlideState::SizeFocus => auto_slides::spawn_size_focus_slide.into_configs(),
        SlideState::StateAmbiguity => auto_slides::spawn_state_ambiguity_slide.into_configs(),
        SlideState::BoolRep => auto_slides::spawn_bool_rep_slide.into_configs(),
        SlideState::ThreadLanes => figure_slides::spawn_thread_lanes_slide.into_configs(),
        SlideState::AbaScenario => figure_slides::spawn_aba_scenario_slide.into_configs(),
        SlideState::MemInit => auto_slides::spawn_mem_init_slide.into_configs(),
        SlideState::AbaProblem => auto_slides::spawn_aba_problem_slide.into_configs(),
        SlideState::ThreadSafety => auto_slides::spawn_thread_safety_slide.into_configs(),
        SlideState::Recovery => auto_slides::spawn_recovery_slide.into_configs(),
        SlideState::AutomateRecovery => auto_slides::spawn_automate_recovery_slide.into_configs(),
        SlideState::CacheContention => auto_slides::spawn_cache_contention_slide.into_configs(),
        SlideState::DisassembledCode => auto_slides::spawn_disassembled_code_slide.into_configs(),
        SlideState::BitWalk => figure_slides::spawn_bit_walk_slide.into_configs(),
        SlideState::PipelineFlush => figure_slides::spawn_pipeline_flush_slide.into_configs(),
        SlideState::BranchlessIndex => auto_slides::spawn_branchless_index_slide.into_configs(),
        SlideState::TailLatency => auto_slides::spawn_tail_latency_slide.into_configs(),
        SlideState::UnboundedLatency => auto_slides::spawn_unbounded_latency_slide.into_configs(),
        SlideState::Backoffs => auto_slides::spawn_backoffs_slide.into_configs(),
        SlideState::CoreTopology => figure_slides::spawn_core_topology_slide.into_configs(),
        SlideState::Scaling => auto_slides::spawn_scaling_slide.into_configs(),
        SlideState::Simplification => auto_slides::spawn_simplification_slide.into_configs(),
    }
}
