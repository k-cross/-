use crate::theme::colors::*;
use crate::theme::motion::*;
use bevy::prelude::*;

/// Fast spring easing function (~15 frames / 0.25s) with snappy overshoot
pub fn ease_out_back(t: f32) -> f32 {
    let c1 = SLAM_OVERSHOOT;
    let c3 = c1 + 1.0;
    1.0 + c3 * (t - 1.0).powi(3) + c1 * (t - 1.0).powi(2)
}

/// Component attached to UI nodes or sprites that violently slap into place on entrance
#[derive(Component, Clone)]
pub struct SlamEntrance {
    pub initial_offset: Vec2,
    pub initial_rot_offset: f32,
    pub target_translation: Vec2,
    pub target_rotation: f32,
    pub target_scale: Vec2,
    pub delay: f32,
    pub duration: f32,
    pub elapsed: f32,
    pub initialized: bool,
}

impl SlamEntrance {
    pub fn new(offset: Vec2, rot_offset: f32, delay: f32) -> Self {
        Self {
            initial_offset: offset,
            initial_rot_offset: rot_offset,
            target_translation: Vec2::ZERO,
            target_rotation: 0.0,
            target_scale: Vec2::ONE,
            delay,
            duration: SLAM_DURATION,
            elapsed: 0.0,
            initialized: false,
        }
    }
}

/// Razor-sharp diagonal screen transition blade wipe
#[derive(Component)]
pub struct ScreenSlashBlade {
    pub timer: f32,
    pub duration: f32,
    pub start_x: f32,
    pub end_x: f32,
}

pub struct AnimationPlugin;

impl Plugin for AnimationPlugin {
    fn build(&self, app: &mut App) {
        app.add_systems(
            Update,
            (
                animate_slam_entrances.run_if(any_with_component::<SlamEntrance>),
                animate_screen_slashes.run_if(any_with_component::<ScreenSlashBlade>),
            ),
        );
    }
}

fn animate_slam_entrances(
    mut commands: Commands,
    time: Res<Time>,
    mut query: Query<(
        Entity,
        Option<&mut UiTransform>,
        Option<&mut Transform>,
        &mut SlamEntrance,
    )>,
) {
    let dt = time.delta_secs();

    for (entity, mut ui_transform, mut sprite_transform, mut slam) in &mut query {
        if !slam.initialized {
            if let Some(ref ui) = ui_transform {
                let tx = match ui.translation.x {
                    Val::Px(v) => v,
                    _ => 0.0,
                };
                let ty = match ui.translation.y {
                    Val::Px(v) => v,
                    _ => 0.0,
                };
                slam.target_translation = Vec2::new(tx, ty);
                slam.target_rotation = ui.rotation.as_radians();
                slam.target_scale = ui.scale;
            } else if let Some(ref tr) = sprite_transform {
                slam.target_translation = tr.translation.truncate();
                slam.target_rotation = tr.rotation.to_euler(EulerRot::ZYX).0;
                slam.target_scale = tr.scale.truncate();
            }

            if let Some(ref mut ui) = ui_transform {
                ui.translation = Val2::px(
                    slam.target_translation.x + slam.initial_offset.x,
                    slam.target_translation.y + slam.initial_offset.y,
                );
                ui.rotation = Rot2::radians(slam.target_rotation + slam.initial_rot_offset);
                ui.scale = slam.target_scale * SLAM_INITIAL_SCALE;
            } else if let Some(ref mut tr) = sprite_transform {
                tr.translation.x += slam.initial_offset.x;
                tr.translation.y += slam.initial_offset.y;
                tr.rotation = Quat::from_rotation_z(slam.target_rotation + slam.initial_rot_offset);
                tr.scale = (slam.target_scale * SLAM_INITIAL_SCALE).extend(1.0);
            }

            slam.initialized = true;
        }

        slam.elapsed += dt;

        if slam.elapsed < slam.delay {
            continue;
        }

        let progress = ((slam.elapsed - slam.delay) / slam.duration).clamp(0.0, 1.0);
        let factor = ease_out_back(progress);

        let remaining_offset = slam.initial_offset * (1.0 - factor);
        let remaining_rot = slam.initial_rot_offset * (1.0 - factor);
        let current_scale = SLAM_INITIAL_SCALE + (1.0 - SLAM_INITIAL_SCALE) * factor;

        if let Some(ref mut ui) = ui_transform {
            ui.translation = Val2::px(
                slam.target_translation.x + remaining_offset.x,
                slam.target_translation.y + remaining_offset.y,
            );
            ui.rotation = Rot2::radians(slam.target_rotation + remaining_rot);
            ui.scale = slam.target_scale * current_scale;

            if progress >= 1.0 {
                ui.translation = Val2::px(slam.target_translation.x, slam.target_translation.y);
                ui.rotation = Rot2::radians(slam.target_rotation);
                ui.scale = slam.target_scale;
                commands.entity(entity).remove::<SlamEntrance>();
            }
        } else if let Some(ref mut tr) = sprite_transform {
            tr.translation.x = slam.target_translation.x + remaining_offset.x;
            tr.translation.y = slam.target_translation.y + remaining_offset.y;
            tr.rotation = Quat::from_rotation_z(slam.target_rotation + remaining_rot);
            tr.scale = (slam.target_scale * current_scale).extend(1.0);

            if progress >= 1.0 {
                tr.translation.x = slam.target_translation.x;
                tr.translation.y = slam.target_translation.y;
                tr.rotation = Quat::from_rotation_z(slam.target_rotation);
                tr.scale = slam.target_scale.extend(1.0);
                commands.entity(entity).remove::<SlamEntrance>();
            }
        }
    }
}

fn animate_screen_slashes(
    mut commands: Commands,
    time: Res<Time>,
    mut query: Query<(Entity, &mut Transform, &mut ScreenSlashBlade)>,
) {
    let dt = time.delta_secs();

    for (entity, mut transform, mut blade) in &mut query {
        blade.timer += dt;
        let progress = (blade.timer / blade.duration).clamp(0.0, 1.0);

        // High velocity non-linear swipe
        let t = progress * progress;
        transform.translation.x = blade.start_x + (blade.end_x - blade.start_x) * t;

        if progress >= 1.0 {
            commands.entity(entity).despawn();
        }
    }
}

/// Spawns a high-speed diagonal crimson blade wipe across the screen on slide change
pub fn spawn_screen_slash(commands: &mut Commands) {
    // 1. Heavy Black Cut Blade
    commands.spawn((
        Sprite {
            color: P5_BLACK,
            custom_size: Some(Vec2::new(3500.0, 120.0)),
            ..default()
        },
        Transform::from_xyz(-1800.0, 0.0, 90.0)
            .with_rotation(Quat::from_rotation_z(SLASH_BLADE_ANGLE)),
        ScreenSlashBlade {
            timer: 0.0,
            duration: SLASH_BLADE_DURATION,
            start_x: -1800.0,
            end_x: 1800.0,
        },
    ));

    // 2. High-Voltage Razor Crimson Edge
    commands.spawn((
        Sprite {
            color: P5_RED,
            custom_size: Some(Vec2::new(3500.0, 16.0)),
            ..default()
        },
        Transform::from_xyz(-1850.0, 40.0, 91.0)
            .with_rotation(Quat::from_rotation_z(SLASH_BLADE_ANGLE)),
        ScreenSlashBlade {
            timer: 0.0,
            duration: SLASH_EDGE_DURATION,
            start_x: -1850.0,
            end_x: 1850.0,
        },
    ));

    // 3. Blinding Stark White Fracture Line
    commands.spawn((
        Sprite {
            color: P5_WHITE,
            custom_size: Some(Vec2::new(3500.0, 6.0)),
            ..default()
        },
        Transform::from_xyz(-1900.0, -30.0, 92.0)
            .with_rotation(Quat::from_rotation_z(SLASH_BLADE_ANGLE)),
        ScreenSlashBlade {
            timer: 0.0,
            duration: SLASH_FRACTURE_DURATION,
            start_x: -1900.0,
            end_x: 1900.0,
        },
    ));
}
