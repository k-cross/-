use bevy::prelude::*;
use crate::theme::colors::*;

#[derive(Component)]
pub struct PhantomThiefRoot {
    pub base_pos: Vec3,
    pub idle_phase: f32,
    pub slash_timer: f32,
    pub slash_duration: f32,
}

#[derive(Component)]
pub struct CoatTail {
    pub base_rot: f32,
    pub sway_speed: f32,
    pub sway_amp: f32,
}

#[derive(Component)]
pub struct DaggerGlint {
    pub pulse_speed: f32,
}

pub struct CharacterPlugin;

impl Plugin for CharacterPlugin {
    fn build(&self, app: &mut App) {
        app.add_systems(Startup, setup_character)
            .add_systems(Update, animate_character);
    }
}

fn setup_character(mut commands: Commands) {
    let root_pos = Vec3::new(440.0, -10.0, -60.0);

    // Root character anchor
    commands.spawn((
        PhantomThiefRoot {
            base_pos: root_pos,
            idle_phase: 0.0,
            slash_timer: 0.0,
            slash_duration: 0.35,
        },
        Transform::from_translation(root_pos),
        Visibility::default(),
        InheritedVisibility::default(),
    )).with_children(|character| {
        // ==============================================================
        // 1. BILLOWING TRENCHCOAT TAILS (Layered sharp black polygons)
        // ==============================================================
        // Main long tail
        character.spawn((
            Sprite {
                color: P5_BLACK,
                custom_size: Some(Vec2::new(140.0, 320.0)),
                ..default()
            },
            Transform::from_xyz(-50.0, -110.0, -1.0)
                .with_rotation(Quat::from_rotation_z(-0.55)),
            CoatTail {
                base_rot: -0.55,
                sway_speed: 4.0,
                sway_amp: 0.08,
            },
        ));

        // Crimson Coat Inner Lining Slash (Sharp high-voltage accent)
        character.spawn((
            Sprite {
                color: P5_RED,
                custom_size: Some(Vec2::new(18.0, 300.0)),
                ..default()
            },
            Transform::from_xyz(-10.0, -115.0, -0.5)
                .with_rotation(Quat::from_rotation_z(-0.54)),
            CoatTail {
                base_rot: -0.54,
                sway_speed: 4.0,
                sway_amp: 0.08,
            },
        ));

        // Secondary flared tail (fluttering back)
        character.spawn((
            Sprite {
                color: P5_OFF_BLACK,
                custom_size: Some(Vec2::new(90.0, 240.0)),
                ..default()
            },
            Transform::from_xyz(-100.0, -70.0, -2.0)
                .with_rotation(Quat::from_rotation_z(-0.85)),
            CoatTail {
                base_rot: -0.85,
                sway_speed: 5.5,
                sway_amp: 0.12,
            },
        ));

        // White torn lining accent stripe
        character.spawn((
            Sprite {
                color: P5_WHITE,
                custom_size: Some(Vec2::new(4.0, 220.0)),
                ..default()
            },
            Transform::from_xyz(-85.0, -75.0, -1.5)
                .with_rotation(Quat::from_rotation_z(-0.85)),
            CoatTail {
                base_rot: -0.85,
                sway_speed: 5.5,
                sway_amp: 0.12,
            },
        ));

        // ==============================================================
        // 2. TORSO & LEGS: High-Contrast Dynamic Lunging Posture
        // ==============================================================
        // Lower legs / boots
        character.spawn((
            Sprite {
                color: P5_BLACK,
                custom_size: Some(Vec2::new(45.0, 180.0)),
                ..default()
            },
            Transform::from_xyz(20.0, -210.0, 1.0)
                .with_rotation(Quat::from_rotation_z(0.20)),
        ));

        // Torso / Popped double-breasted coat
        character.spawn((
            Sprite {
                color: P5_BLACK,
                custom_size: Some(Vec2::new(100.0, 150.0)),
                ..default()
            },
            Transform::from_xyz(10.0, 10.0, 2.0)
                .with_rotation(Quat::from_rotation_z(-0.18)),
        ));

        // Stark Popped Coat Lapel (Sharp jagged collar)
        character.spawn((
            Sprite {
                color: P5_CHARCOAL,
                custom_size: Some(Vec2::new(35.0, 80.0)),
                ..default()
            },
            Transform::from_xyz(-22.0, 75.0, 3.0)
                .with_rotation(Quat::from_rotation_z(-0.45)),
        ));

        // Red cravat / necktie accent
        character.spawn((
            Sprite {
                color: P5_RED,
                custom_size: Some(Vec2::new(16.0, 45.0)),
                ..default()
            },
            Transform::from_xyz(6.0, 62.0, 3.5)
                .with_rotation(Quat::from_rotation_z(-0.15)),
        ));

        // ==============================================================
        // 3. HEAD & THE ICONIC PHANTOM MASK
        // ==============================================================
        // Head / ruffled hair silhouette
        character.spawn((
            Sprite {
                color: P5_BLACK,
                custom_size: Some(Vec2::new(70.0, 75.0)),
                ..default()
            },
            Transform::from_xyz(-2.0, 125.0, 4.0)
                .with_rotation(Quat::from_rotation_z(-0.12)),
        ));

        // Stark White Domino Mask Cutout
        character.spawn((
            Sprite {
                color: P5_WHITE,
                custom_size: Some(Vec2::new(52.0, 24.0)),
                ..default()
            },
            Transform::from_xyz(-12.0, 128.0, 5.0)
                .with_rotation(Quat::from_rotation_z(-0.14)),
        ));

        // Mask Eye Slits (Deep pitch black)
        character.spawn((
            Sprite {
                color: P5_BLACK,
                custom_size: Some(Vec2::new(14.0, 6.0)),
                ..default()
            },
            Transform::from_xyz(-22.0, 129.0, 6.0)
                .with_rotation(Quat::from_rotation_z(-0.08)),
        ));
        character.spawn((
            Sprite {
                color: P5_BLACK,
                custom_size: Some(Vec2::new(14.0, 6.0)),
                ..default()
            },
            Transform::from_xyz(-2.0, 127.0, 6.0)
                .with_rotation(Quat::from_rotation_z(-0.18)),
        ));

        // Piercing Crimson Eye Glints (Persona 5 eye flash!)
        character.spawn((
            Sprite {
                color: P5_BRIGHT_RED,
                custom_size: Some(Vec2::new(5.0, 5.0)),
                ..default()
            },
            Transform::from_xyz(-21.0, 129.0, 7.0),
        ));
        character.spawn((
            Sprite {
                color: P5_BRIGHT_RED,
                custom_size: Some(Vec2::new(5.0, 5.0)),
                ..default()
            },
            Transform::from_xyz(-1.0, 127.0, 7.0),
        ));

        // ==============================================================
        // 4. OUTSTRETCHED ARM & GLEAMING RED-EDGED DAGGER
        // ==============================================================
        // Arm thrusting forward towards the center of the screen
        character.spawn((
            Sprite {
                color: P5_BLACK,
                custom_size: Some(Vec2::new(130.0, 36.0)),
                ..default()
            },
            Transform::from_xyz(-70.0, 35.0, 4.0)
                .with_rotation(Quat::from_rotation_z(0.32)),
        ));

        // Crimson gloved hand
        character.spawn((
            Sprite {
                color: P5_RED,
                custom_size: Some(Vec2::new(32.0, 26.0)),
                ..default()
            },
            Transform::from_xyz(-135.0, 60.0, 5.0)
                .with_rotation(Quat::from_rotation_z(0.40)),
        ));

        // Gleaming Stiletto Dagger (Stark white blade with razor crimson back)
        character.spawn((
            Sprite {
                color: P5_WHITE,
                custom_size: Some(Vec2::new(95.0, 10.0)),
                ..default()
            },
            Transform::from_xyz(-185.0, 80.0, 6.0)
                .with_rotation(Quat::from_rotation_z(0.52)),
        ));

        // Crimson Blade Edge / Bloodline
        character.spawn((
            Sprite {
                color: P5_RED,
                custom_size: Some(Vec2::new(95.0, 3.0)),
                ..default()
            },
            Transform::from_xyz(-184.0, 84.0, 6.5)
                .with_rotation(Quat::from_rotation_z(0.52)),
            DaggerGlint { pulse_speed: 6.0 },
        ));

        // Sharp Starburst Glint on Dagger Tip
        character.spawn((
            Sprite {
                color: P5_WHITE,
                custom_size: Some(Vec2::new(16.0, 16.0)),
                ..default()
            },
            Transform::from_xyz(-228.0, 104.0, 7.0)
                .with_rotation(Quat::from_rotation_z(0.78)),
            DaggerGlint { pulse_speed: 8.0 },
        ));
    });
}

fn animate_character(
    time: Res<Time>,
    mut roots: Query<(&mut Transform, &mut PhantomThiefRoot)>,
    mut tails: Query<(&mut Transform, &CoatTail), Without<PhantomThiefRoot>>,
    mut glints: Query<(&mut Transform, &DaggerGlint), (Without<PhantomThiefRoot>, Without<CoatTail>)>,
) {
    let dt = time.delta_secs();

    // 1. Root Character Idle Breathing & Violent Slash Lunge
    for (mut transform, mut root) in &mut roots {
        root.idle_phase += dt * 3.0;

        let idle_offset_y = (root.idle_phase).sin() * 5.0;
        let idle_rot = (root.idle_phase * 0.7).cos() * 0.015;

        if root.slash_timer > 0.0 {
            root.slash_timer -= dt;
            let slash_t = (1.0 - (root.slash_timer / root.slash_duration)).clamp(0.0, 1.0);

            // Explosive forward lunge and swift snap-back
            let lunge_factor = if slash_t < 0.3 {
                // Explosive forward thrust
                slash_t / 0.3
            } else {
                // Snappy recoil
                1.0 - (slash_t - 0.3) / 0.7
            };

            let lunge_x = -lunge_factor * 60.0;
            let lunge_rot = -lunge_factor * 0.08;

            transform.translation.x = root.base_pos.x + lunge_x;
            transform.translation.y = root.base_pos.y + idle_offset_y;
            transform.rotation = Quat::from_rotation_z(idle_rot + lunge_rot);
        } else {
            transform.translation.x = root.base_pos.x;
            transform.translation.y = root.base_pos.y + idle_offset_y;
            transform.rotation = Quat::from_rotation_z(idle_rot);
        }
    }

    // 2. Coat Tails Fluttering in Dynamic Wind
    let elapsed = time.elapsed_secs();
    for (mut transform, tail) in &mut tails {
        let flutter = (elapsed * tail.sway_speed).sin() * tail.sway_amp;
        transform.rotation = Quat::from_rotation_z(tail.base_rot + flutter);
    }

    // 3. Dagger Glint Pulse
    for (mut transform, glint) in &mut glints {
        let scale = 1.0 + (elapsed * glint.pulse_speed).sin().abs() * 0.35;
        transform.scale = Vec3::splat(scale);
    }
}

/// Trigger the character's swift forward slash/lunge
pub fn trigger_character_slash(mut roots: Query<&mut PhantomThiefRoot>) {
    for mut root in &mut roots {
        root.slash_timer = root.slash_duration;
    }
}
