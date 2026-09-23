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
