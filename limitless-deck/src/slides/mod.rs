pub mod intro;
pub mod mutex_bottleneck;

use crate::slideshow::SlideState;
use bevy::prelude::*;

pub struct SlidesPlugin;

impl Plugin for SlidesPlugin {
    fn build(&self, app: &mut App) {
        app.add_systems(OnEnter(SlideState::Intro), intro::spawn_intro_slide)
            .add_systems(
                OnEnter(SlideState::MutexBottleneck),
                mutex_bottleneck::spawn_mutex_bottleneck_slide,
            );
    }
}
