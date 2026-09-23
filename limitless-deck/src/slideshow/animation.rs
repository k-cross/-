use bevy::prelude::*;

/// Fast spring easing function (~15 frames / 0.25s) with snappy overshoot
pub fn ease_out_back(t: f32) -> f32 {
    let c1 = 1.70158;
    let c3 = c1 + 1.0;
    1.0 + c3 * (t - 1.0).powi(3) + c1 * (t - 1.0).powi(2)
}

/// Component attached to UI nodes or sprites that violently slap into place on entrance
#[derive(Component, Clone)]
pub struct SlamEntrance {
    pub initial_offset: Vec2,
    pub initial_rot_offset: f32,
    pub target_translation: Vec3,
    pub target_rotation: Quat,
    pub target_scale: Vec3,
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
            target_translation: Vec3::ZERO,
            target_rotation: Quat::IDENTITY,
            target_scale: Vec3::ONE,
            delay,
            duration: 0.28, // ~17 frames at 60fps - ultra snappy!
            elapsed: 0.0,
            initialized: false,
        }
    }
}

/// Component providing erratic living micro-jitter and breathing for punk aesthetics
#[derive(Component)]
pub struct PunkJitter {
    pub base_rotation: f32,
    pub amplitude: f32,
    pub frequency: f32,
    pub phase: f32,
    pub twitch_timer: f32,
    pub current_twitch: f32,
}

impl PunkJitter {
    pub fn new(base_rotation: f32, amplitude: f32) -> Self {
        Self {
            base_rotation,
            amplitude,
            frequency: 6.0,
            phase: 0.0,
            twitch_timer: 0.5,
            current_twitch: 0.0,
        }
    }
}

/// Component for animated bobbing selection cursor (dagger/arrow)
#[derive(Component)]
pub struct BobbingCursor {
    pub speed: f32,
    pub distance: f32,
}

impl Default for BobbingCursor {
    fn default() -> Self {
        Self {
            speed: 8.0,
            distance: 5.0,
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
                animate_slam_entrances,
                animate_punk_jitter,
                animate_bobbing_cursors,
                animate_screen_slashes,
            ),
        );
    }
}

fn animate_slam_entrances(
    mut commands: Commands,
    time: Res<Time>,
    mut query: Query<(Entity, &mut Transform, &mut SlamEntrance)>,
) {
    let dt = time.delta_secs();

    for (entity, mut transform, mut slam) in &mut query {
        if !slam.initialized {
            slam.target_translation = transform.translation;
            slam.target_rotation = transform.rotation;
            slam.target_scale = transform.scale;

            // Start far off along the violent slam trajectory
            transform.translation += Vec3::new(slam.initial_offset.x, slam.initial_offset.y, 0.0);
            transform.rotation = slam.target_rotation * Quat::from_rotation_z(slam.initial_rot_offset);
            transform.scale = slam.target_scale * 0.4; // burst up from compact scale

            slam.initialized = true;
        }

        slam.elapsed += dt;

        if slam.elapsed < slam.delay {
            // Still waiting for staggered entrance
            continue;
        }

        let progress = ((slam.elapsed - slam.delay) / slam.duration).clamp(0.0, 1.0);
        let factor = ease_out_back(progress);

        // Interpolate position with spring bounce
        let remaining_offset = slam.initial_offset * (1.0 - factor);
        transform.translation = slam.target_translation + Vec3::new(remaining_offset.x, remaining_offset.y, 0.0);

        // Interpolate rotation
        let remaining_rot = slam.initial_rot_offset * (1.0 - factor);
        transform.rotation = slam.target_rotation * Quat::from_rotation_z(remaining_rot);

        // Interpolate scale
        let current_scale = 0.4 + 0.6 * factor;
        transform.scale = slam.target_scale * current_scale;

        if progress >= 1.0 {
            // Snap cleanly to target transform and remove slam component
            transform.translation = slam.target_translation;
            transform.rotation = slam.target_rotation;
            transform.scale = slam.target_scale;
            commands.entity(entity).remove::<SlamEntrance>();
        }
    }
}

fn animate_punk_jitter(
    time: Res<Time>,
    mut query: Query<(&mut Transform, &mut PunkJitter), Without<SlamEntrance>>,
) {
    let dt = time.delta_secs();

    for (mut transform, mut jitter) in &mut query {
        jitter.phase += dt * jitter.frequency;
        jitter.twitch_timer -= dt;

        // Occasional sharp twitch every 0.4 - 0.9s
        if jitter.twitch_timer <= 0.0 {
            jitter.twitch_timer = 0.4 + (jitter.phase.sin().abs() * 0.5);
            // Erratic micro-twitch
            jitter.current_twitch = (jitter.phase * 3.14).sin() * 0.015;
        } else {
            // Decay twitch back towards zero
            jitter.current_twitch *= (1.0 - dt * 8.0).max(0.0);
        }

        let organic_sine = (jitter.phase).sin() * jitter.amplitude;
        let total_rot = jitter.base_rotation + organic_sine + jitter.current_twitch;
        transform.rotation = Quat::from_rotation_z(total_rot);

        // Micro-pulse scale (1.0 to 1.015)
        let pulse = 1.0 + (jitter.phase * 1.5).sin().abs() * 0.015;
        transform.scale = Vec3::splat(pulse);
    }
}

fn animate_bobbing_cursors(
    time: Res<Time>,
    mut query: Query<(&mut Node, &BobbingCursor)>,
) {
    let elapsed = time.elapsed_secs();

    for (mut node, cursor) in &mut query {
        let offset = ((elapsed * cursor.speed).sin().abs()) * cursor.distance;
        node.margin.left = Val::Px(offset);
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
            color: Color::srgb(0.05, 0.05, 0.07),
            custom_size: Some(Vec2::new(3500.0, 120.0)),
            ..default()
        },
        Transform::from_xyz(-1800.0, 0.0, 90.0)
            .with_rotation(Quat::from_rotation_z(-0.40)),
        ScreenSlashBlade {
            timer: 0.0,
            duration: 0.22,
            start_x: -1800.0,
            end_x: 1800.0,
        },
    ));

    // 2. High-Voltage Razor Crimson Edge
    commands.spawn((
        Sprite {
            color: Color::srgb(0.902, 0.0, 0.071),
            custom_size: Some(Vec2::new(3500.0, 16.0)),
            ..default()
        },
        Transform::from_xyz(-1850.0, 40.0, 91.0)
            .with_rotation(Quat::from_rotation_z(-0.40)),
        ScreenSlashBlade {
            timer: 0.0,
            duration: 0.20,
            start_x: -1850.0,
            end_x: 1850.0,
        },
    ));

    // 3. Blinding Stark White Fracture Line
    commands.spawn((
        Sprite {
            color: Color::WHITE,
            custom_size: Some(Vec2::new(3500.0, 6.0)),
            ..default()
        },
        Transform::from_xyz(-1900.0, -30.0, 92.0)
            .with_rotation(Quat::from_rotation_z(-0.40)),
        ScreenSlashBlade {
            timer: 0.0,
            duration: 0.18,
            start_x: -1900.0,
            end_x: 1900.0,
        },
    ));
}
