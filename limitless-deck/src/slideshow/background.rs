use bevy::prelude::*;
use crate::theme::colors::*;
use crate::theme::captivity::*;
use crate::theme::ink;

#[derive(Component)]
pub struct BackgroundShard {
    pub velocity: Vec2,
    pub rot_speed: f32,
    pub bounds: Vec2,
}

#[derive(Component)]
pub struct ShatteredChainLink {
    pub velocity: Vec2,
    pub rot_speed: f32,
    pub bounds: Vec2,
}

pub struct BackgroundPlugin;

impl Plugin for BackgroundPlugin {
    fn build(&self, app: &mut App) {
        app.add_systems(Startup, setup_background)
            .add_systems(Update, (animate_background, animate_shattered_chains));
    }
}

fn setup_background(mut commands: Commands) {
    // 1. Base deep black backdrop
    commands.spawn((
        Sprite {
            color: P5_BLACK,
            custom_size: Some(Vec2::new(3000.0, 3000.0)),
            ..default()
        },
        Transform::from_xyz(0.0, 0.0, -100.0),
    ));

    // 2. Gritty dark charcoal diagonal band
    commands.spawn((
        Sprite {
            color: P5_CHARCOAL,
            custom_size: Some(Vec2::new(3500.0, 260.0)),
            ..default()
        },
        Transform::from_xyz(100.0, -60.0, -95.0)
            .with_rotation(Quat::from_rotation_z(STRIPE_ANGLE)),
    ));

    // ==============================================================
    // PRISON UNIFORM / CAPTIVITY HAZARD STRIPES (Black & White Bars)
    // ==============================================================
    // A diagonal ribbon of alternating stark black and white hazard bars
    for i in 0..STRIPE_COUNT {
        let offset = (i as f32 - (STRIPE_COUNT as f32 / 2.0)) * STRIPE_SPACING;
        let is_white = i % 2 == 0;
        let color = if is_white { P5_WHITE } else { P5_OFF_BLACK };

        commands.spawn((
            Sprite {
                color,
                custom_size: Some(Vec2::new(STRIPE_WIDTH, STRIPE_HEIGHT)),
                ..default()
            },
            Transform::from_xyz(offset, 40.0 + (offset * -0.36), -90.0)
                .with_rotation(Quat::from_rotation_z(STRIPE_ANGLE)),
        ));
    }

    // Razor-sharp crimson punk accent slash (electric accent bounding the prison stripes)
    commands.spawn((
        Sprite {
            color: P5_RED,
            custom_size: Some(Vec2::new(3500.0, 7.0)),
            ..default()
        },
        Transform::from_xyz(100.0, 54.0, -85.0)
            .with_rotation(Quat::from_rotation_z(STRIPE_ANGLE)),
    ));

    // Secondary bright crimson hair-line
    commands.spawn((
        Sprite {
            color: P5_BRIGHT_RED,
            custom_size: Some(Vec2::new(3500.0, 2.5)),
            ..default()
        },
        Transform::from_xyz(100.0, 66.0, -84.0)
            .with_rotation(Quat::from_rotation_z(STRIPE_ANGLE)),
    ));

    // Stark white torn-edge tape line
    commands.spawn((
        Sprite {
            color: P5_WHITE,
            custom_size: Some(Vec2::new(3500.0, 3.5)),
            ..default()
        },
        Transform::from_xyz(100.0, -180.0, -85.0)
            .with_rotation(Quat::from_rotation_z(STRIPE_ANGLE)),
    ));

    // ==============================================================
    // 3. SHATTERED CHAINS (Broken links of captivity tumbling in space)
    // ==============================================================
    let chain_configs = [
        (Vec2::new(-380.0, 180.0), 0.6, Vec2::new(18.0, -10.0), 0.8),
        (Vec2::new(-200.0, -160.0), -0.4, Vec2::new(-12.0, 14.0), -1.1),
        (Vec2::new(280.0, 210.0), 0.9, Vec2::new(-15.0, -8.0), 0.6),
        (Vec2::new(480.0, -140.0), -0.7, Vec2::new(10.0, 12.0), -0.9),
    ];

    for (pos, angle, vel, rot_speed) in chain_configs {
        // Spawn a shattered chain link: 4 outer bars and broken open gap
        commands.spawn((
            Sprite {
                color: P5_LIGHT_GREY,
                custom_size: Some(Vec2::new(CHAIN_WIDTH, CHAIN_HEIGHT)),
                ..default()
            },
            Transform::from_xyz(pos.x, pos.y, -72.0)
                .with_rotation(Quat::from_rotation_z(angle)),
            ShatteredChainLink {
                velocity: vel,
                rot_speed,
                bounds: Vec2::new(750.0, 420.0),
            },
            Visibility::default(),
            InheritedVisibility::default(),
        )).with_children(|chain| {
            // Cutout hole
            chain.spawn((
                Sprite {
                    color: P5_BLACK,
                    custom_size: Some(Vec2::new(CHAIN_CUTOUT_WIDTH, CHAIN_CUTOUT_HEIGHT)),
                    ..default()
                },
                Transform::from_xyz(0.0, 0.0, 1.0),
            ));
            // Crimson snap fracture line across the break
            chain.spawn((
                Sprite {
                    color: P5_RED,
                    custom_size: Some(Vec2::new(FRACTURE_WIDTH, FRACTURE_HEIGHT)),
                    ..default()
                },
                Transform::from_xyz(FRACTURE_OFFSET_X, 0.0, 2.0)
                    .with_rotation(Quat::from_rotation_z(FRACTURE_ANGLE)),
            ));
        });
    }

    // ==============================================================
    // 4. FLOATING GEOMETRIC REBEL SHARDS (Diamond & polygon drift)
    // ==============================================================
    let shard_configs = [
        (Vec2::new(-450.0, 260.0), Vec2::new(45.0, 45.0), P5_RED, 0.45, Vec2::new(14.0, -7.0), 0.5),
        (Vec2::new(520.0, 220.0), Vec2::new(30.0, 30.0), P5_WHITE, 0.65, Vec2::new(-12.0, 9.0), -0.7),
        (Vec2::new(-350.0, -220.0), Vec2::new(60.0, 60.0), P5_OFF_BLACK, 0.85, Vec2::new(9.0, 6.0), 0.3),
        (Vec2::new(400.0, -180.0), Vec2::new(25.0, 25.0), P5_LIGHT_GREY, 0.55, Vec2::new(-16.0, -11.0), 1.2),
        (Vec2::new(-100.0, 300.0), Vec2::new(35.0, 35.0), P5_RED, 0.35, Vec2::new(6.0, -14.0), -0.4),
        (Vec2::new(200.0, 320.0), Vec2::new(50.0, 12.0), P5_WHITE, 0.45, Vec2::new(-9.0, 7.0), 0.8),
        (Vec2::new(-500.0, 50.0), Vec2::new(40.0, 10.0), P5_GREY, 0.55, Vec2::new(15.0, 5.0), -0.6),
    ];

    for (pos, size, color, alpha, vel, rot_speed) in shard_configs {
        let mut final_color = color;
        final_color.set_alpha(alpha);

        commands.spawn((
            Sprite {
                color: final_color,
                custom_size: Some(size),
                ..default()
            },
            Transform::from_xyz(pos.x, pos.y, -70.0)
                .with_rotation(Quat::from_rotation_z(0.2)),
            BackgroundShard {
                velocity: vel,
                rot_speed,
                bounds: Vec2::new(800.0, 450.0),
            },
        ));
    }

    // 5. Persona 5 Comic Speed-Slits
    for i in 0..8 {
        let offset = i as f32 * 18.0;
        // Top-left comic diagonal slits
        commands.spawn((
            Sprite {
                color: Color::srgba(1.0, 1.0, 1.0, 0.08),
                custom_size: Some(Vec2::new(44.0, 2.2)),
                ..default()
            },
            Transform::from_xyz(-580.0 + offset, 300.0 - offset * 0.5, -80.0)
                .with_rotation(Quat::from_rotation_z(0.6)),
        ));
        // Bottom-right crimson comic slits
        commands.spawn((
            Sprite {
                color: Color::srgba(0.902, 0.0, 0.071, 0.22),
                custom_size: Some(Vec2::new(52.0, 2.8)),
                ..default()
            },
            Transform::from_xyz(540.0 - offset, -290.0 + offset * 0.5, -80.0)
                .with_rotation(Quat::from_rotation_z(0.6)),
        ));
    }

    // ==============================================================
    // 6. ORGANIC INK BLOTCHES & HIGH-VELOCITY SPRAY SPLATTERS
    // ==============================================================
    // Deep black ink pool anchoring the title scrap area
    crate::slideshow::splatter::spawn_ink_blotch(
        &mut commands,
        Vec2::new(-350.0, 70.0),
        -86.0,
        P5_OFF_BLACK,
        ink::RADIUS_XL,
        Vec2::new(1.2, -0.4),
    );

    // Electric crimson splatter cutting under the divider
    crate::slideshow::splatter::spawn_ink_blotch(
        &mut commands,
        Vec2::new(-200.0, -120.0),
        -82.0,
        P5_RED,
        45.0,
        Vec2::new(1.5, 0.3),
    );

    // Charcoal ink stain pooling under the calling card
    crate::slideshow::splatter::spawn_ink_blotch(
        &mut commands,
        Vec2::new(320.0, 70.0),
        -86.0,
        P5_CHARCOAL,
        ink::RADIUS_LG,
        Vec2::new(-1.0, -0.7),
    );

    // Crimson spray splatter near bottom-right
    crate::slideshow::splatter::spawn_ink_blotch(
        &mut commands,
        Vec2::new(450.0, -180.0),
        -82.0,
        P5_DARK_RED,
        38.0,
        Vec2::new(-0.8, 1.4),
    );
}

fn animate_background(
    time: Res<Time>,
    mut query: Query<(&mut Transform, &BackgroundShard)>,
) {
    let dt = time.delta_secs();
    for (mut transform, shard) in &mut query {
        transform.translation.x += shard.velocity.x * dt;
        transform.translation.y += shard.velocity.y * dt;
        transform.rotate_z(shard.rot_speed * dt);

        if transform.translation.x > shard.bounds.x {
            transform.translation.x = -shard.bounds.x;
        } else if transform.translation.x < -shard.bounds.x {
            transform.translation.x = shard.bounds.x;
        }

        if transform.translation.y > shard.bounds.y {
            transform.translation.y = -shard.bounds.y;
        } else if transform.translation.y < -shard.bounds.y {
            transform.translation.y = shard.bounds.y;
        }
    }
}

fn animate_shattered_chains(
    time: Res<Time>,
    mut query: Query<(&mut Transform, &ShatteredChainLink)>,
) {
    let dt = time.delta_secs();
    for (mut transform, chain) in &mut query {
        transform.translation.x += chain.velocity.x * dt;
        transform.translation.y += chain.velocity.y * dt;
        transform.rotate_z(chain.rot_speed * dt);

        if transform.translation.x > chain.bounds.x {
            transform.translation.x = -chain.bounds.x;
        } else if transform.translation.x < -chain.bounds.x {
            transform.translation.x = chain.bounds.x;
        }

        if transform.translation.y > chain.bounds.y {
            transform.translation.y = -chain.bounds.y;
        } else if transform.translation.y < -chain.bounds.y {
            transform.translation.y = chain.bounds.y;
        }
    }
}
