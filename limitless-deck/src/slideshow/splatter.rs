use bevy::prelude::*;
use crate::theme::ink::*;

#[derive(Component)]
pub struct InkSplatterRoot;

/// Spawns an organic ink blotch with a central pooling nucleus and radiating satellite droplets
pub fn spawn_ink_blotch(
    commands: &mut Commands,
    center: Vec2,
    z: f32,
    color: Color,
    radius: f32,
    spray_dir: Vec2,
) -> Entity {
    // Root container
    commands.spawn((
        Transform::from_xyz(center.x, center.y, z),
        Visibility::default(),
        InheritedVisibility::default(),
        InkSplatterRoot,
    )).with_children(|blotch| {
        // 1. Central pooling nucleus (layered overlapping irregular shapes)
        blotch.spawn((
            Sprite {
                color,
                custom_size: Some(Vec2::new(radius * NUCLEUS_A.0, radius * NUCLEUS_A.1)),
                ..default()
            },
            Transform::from_xyz(0.0, 0.0, 0.0)
                .with_rotation(Quat::from_rotation_z(NUCLEUS_ROT_A)),
        ));

        blotch.spawn((
            Sprite {
                color,
                custom_size: Some(Vec2::new(radius * NUCLEUS_B.0, radius * NUCLEUS_B.1)),
                ..default()
            },
            Transform::from_xyz(radius * NUCLEUS_B_OFFSET.0, radius * NUCLEUS_B_OFFSET.1, 0.1)
                .with_rotation(Quat::from_rotation_z(NUCLEUS_ROT_B)),
        ));

        blotch.spawn((
            Sprite {
                color,
                custom_size: Some(Vec2::new(radius * NUCLEUS_C.0, radius * NUCLEUS_C.1)),
                ..default()
            },
            Transform::from_xyz(radius * NUCLEUS_C_OFFSET.0, radius * NUCLEUS_C_OFFSET.1, 0.2)
                .with_rotation(Quat::from_rotation_z(NUCLEUS_ROT_C)), // 45 deg diamond point
        ));

        // 2. High-velocity radiating droplets and spray specks
        let norm_spray = if spray_dir.length_squared() > 0.001 {
            spray_dir.normalize()
        } else {
            Vec2::new(1.0, 0.5).normalize()
        };

        // Radiating satellite drops
        let drops = [
            (norm_spray * (radius * 1.2) + Vec2::new(5.0, -8.0), radius * 0.35, 0.2),
            (norm_spray * (radius * 1.7) + Vec2::new(-6.0, 10.0), radius * 0.28, -0.4),
            (norm_spray * (radius * 2.3) + Vec2::new(8.0, 4.0), radius * 0.22, 0.6),
            (norm_spray * (radius * 2.9) + Vec2::new(-4.0, -6.0), radius * 0.16, -0.1),
            // Backsplash micro-specks (opposite to spray direction)
            (-norm_spray * (radius * 0.9) + Vec2::new(7.0, 5.0), radius * 0.24, 0.5),
            (-norm_spray * (radius * 1.3) + Vec2::new(-5.0, -7.0), radius * 0.18, -0.3),
            // Lateral flying splatter dots
            (Vec2::new(-norm_spray.y, norm_spray.x) * (radius * 1.1) + Vec2::new(3.0, 2.0), radius * 0.25, 0.8),
            (Vec2::new(norm_spray.y, -norm_spray.x) * (radius * 1.2) + Vec2::new(-4.0, -3.0), radius * 0.20, -0.7),
            (Vec2::new(-norm_spray.y, norm_spray.x) * (radius * 1.8) + Vec2::new(6.0, 8.0), radius * 0.14, 0.4),
            (Vec2::new(norm_spray.y, -norm_spray.x) * (radius * 2.0) + Vec2::new(-8.0, -6.0), radius * 0.12, -0.5),
        ];

        for (pos, size, rot) in drops {
            blotch.spawn((
                Sprite {
                    color,
                    custom_size: Some(Vec2::new(size, size)),
                    ..default()
                },
                Transform::from_xyz(pos.x, pos.y, DROPLET_Z)
                    .with_rotation(Quat::from_rotation_z(rot)),
            ));
        }
    }).id()
}

/// Spawns an organic fluid ink splatter directly in the UI hierarchy using antialiased circular nodes.
/// Multi-lobed pooling center and radiating droplet spray create an unmistakable ink puddle effect.
pub fn spawn_ui_ink_blotch(
    parent: &mut ChildSpawnerCommands,
    pos: Vec2,
    radius: f32,
    color: Color,
    spray_dir: Vec2,
) {
    parent.spawn((
        Node {
            position_type: PositionType::Absolute,
            left: Val::Px(pos.x),
            top: Val::Px(pos.y),
            width: Val::Px(0.0),
            height: Val::Px(0.0),
            ..default()
        },
        Visibility::default(),
        InheritedVisibility::default(),
    )).with_children(|blotch| {
        // Multi-lobed pooling center made of overlapping circular nodes (antialiased round ink puddle)
        let lobes = [
            (Vec2::ZERO, radius * 1.8),
            (Vec2::new(radius * 0.35, -radius * 0.2), radius * 1.4),
            (Vec2::new(-radius * 0.3, radius * 0.25), radius * 1.3),
            (Vec2::new(radius * 0.15, radius * 0.4), radius * 1.15),
            (Vec2::new(-radius * 0.35, -radius * 0.25), radius * 1.05),
            (Vec2::new(radius * 0.5, radius * 0.1), radius * 0.9),
        ];

        for (offset, size) in lobes {
            blotch.spawn((
                Node {
                    position_type: PositionType::Absolute,
                    left: Val::Px(offset.x - size * 0.5),
                    top: Val::Px(offset.y - size * 0.5),
                    width: Val::Px(size),
                    height: Val::Px(size),
                    border_radius: BorderRadius::MAX,
                    ..default()
                },
                BackgroundColor(color),
            ));
        }

        // High-velocity radiating circular droplets
        let dir = if spray_dir.length_squared() > 0.001 {
            spray_dir.normalize()
        } else {
            Vec2::new(1.0, 0.5).normalize()
        };
        let perp = Vec2::new(-dir.y, dir.x);

        let droplets = [
            (dir * (radius * 1.25) + perp * 6.0, radius * 0.38),
            (dir * (radius * 1.7) - perp * 5.0, radius * 0.30),
            (dir * (radius * 2.3) + perp * 9.0, radius * 0.24),
            (dir * (radius * 2.9) - perp * 4.0, radius * 0.18),
            (dir * (radius * 3.5) + perp * 2.0, radius * 0.12),
            // Backsplash micro-specks
            (-dir * (radius * 0.85) - perp * 4.0, radius * 0.26),
            (-dir * (radius * 1.25) + perp * 6.0, radius * 0.18),
            // Lateral flying splatter dots
            (perp * (radius * 1.05) + dir * 5.0, radius * 0.24),
            (-perp * (radius * 1.05) - dir * 3.0, radius * 0.20),
            (perp * (radius * 1.55) - dir * 7.0, radius * 0.15),
            (-perp * (radius * 1.65) + dir * 8.0, radius * 0.13),
        ];

        for (d_pos, d_size) in droplets {
            blotch.spawn((
                Node {
                    position_type: PositionType::Absolute,
                    left: Val::Px(d_pos.x - d_size * 0.5),
                    top: Val::Px(d_pos.y - d_size * 0.5),
                    width: Val::Px(d_size),
                    height: Val::Px(d_size),
                    border_radius: BorderRadius::MAX,
                    ..default()
                },
                BackgroundColor(color),
            ));
        }
    });
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CardCorner {
    TopLeft,
    TopRight,
    BottomLeft,
    BottomRight,
}

/// Stamps an organic fluid ink splatter directly on the corner of a UI card
/// (replicating the corner-stamped cards from Persona 5, such as the Iwai Airsoft Shop menu).
/// Bleeds fluidly across the card border with overlapping circular lobes and radiating droplets.
pub fn spawn_corner_ink_splatter(
    card: &mut ChildSpawnerCommands,
    corner: CardCorner,
    color: Color,
    size: f32,
) {
    let (center_left, center_top, spray_dir) = match corner {
        CardCorner::TopLeft => (Val::Px(-size * 0.25), Val::Px(-size * 0.25), Vec2::new(1.0, 0.8)),
        CardCorner::TopRight => (Val::Px(size * 0.25), Val::Px(-size * 0.25), Vec2::new(-1.0, 0.8)),
        CardCorner::BottomLeft => (Val::Px(-size * 0.25), Val::Px(size * 0.25), Vec2::new(1.0, -0.8)),
        CardCorner::BottomRight => (Val::Px(size * 0.25), Val::Px(size * 0.25), Vec2::new(-1.0, -0.8)),
    };

    let dir = spray_dir.normalize();
    let perp = Vec2::new(-dir.y, dir.x);

    card.spawn((
        Node {
            position_type: PositionType::Absolute,
            left: center_left,
            top: center_top,
            width: Val::Px(0.0),
            height: Val::Px(0.0),
            ..default()
        },
        Visibility::default(),
        InheritedVisibility::default(),
    )).with_children(|splat| {
        // Central overlapping circular lobes forming the corner ink pool
        let lobes = [
            (Vec2::ZERO, size * 1.0),
            (dir * (size * 0.25), size * 0.8),
            (perp * (size * 0.2), size * 0.7),
            (-perp * (size * 0.2), size * 0.65),
            (dir * (size * 0.45) + perp * (size * 0.1), size * 0.55),
        ];

        for (offset, lobe_size) in lobes {
            splat.spawn((
                Node {
                    position_type: PositionType::Absolute,
                    left: Val::Px(offset.x - lobe_size * 0.5),
                    top: Val::Px(offset.y - lobe_size * 0.5),
                    width: Val::Px(lobe_size),
                    height: Val::Px(lobe_size),
                    border_radius: BorderRadius::MAX,
                    ..default()
                },
                BackgroundColor(color),
            ));
        }

        // Radiating satellite droplets bleeding across the card boundary
        let droplets = [
            (dir * (size * 0.75) + perp * 4.0, size * 0.24),
            (dir * (size * 1.1) - perp * 3.0, size * 0.18),
            (dir * (size * 1.5) + perp * 6.0, size * 0.14),
            (-dir * (size * 0.6) + perp * 4.0, size * 0.20),
            (perp * (size * 0.7) - dir * 2.0, size * 0.16),
            (-perp * (size * 0.7) + dir * 3.0, size * 0.15),
        ];

        for (d_pos, d_size) in droplets {
            splat.spawn((
                Node {
                    position_type: PositionType::Absolute,
                    left: Val::Px(d_pos.x - d_size * 0.5),
                    top: Val::Px(d_pos.y - d_size * 0.5),
                    width: Val::Px(d_size),
                    height: Val::Px(d_size),
                    border_radius: BorderRadius::MAX,
                    ..default()
                },
                BackgroundColor(color),
            ));
        }
    });
}


