/// Persona 5 inspired color palette strictly constrained to:
/// Reds, Whites, Blacks, and Greys.
#[allow(dead_code)]
pub mod colors {
    use bevy::prelude::*;

    // --- REDS ---
    /// Iconic Persona 5 Phantom Crimson Red (#E60012)
    pub const P5_RED: Color = Color::srgb(0.902, 0.0, 0.071);

    /// Deep Crimson for shadows and contrast (#8B0008)
    pub const P5_DARK_RED: Color = Color::srgb(0.545, 0.0, 0.031);

    /// Intense Bright Crimson for highlights (#FF1A2B)
    pub const P5_BRIGHT_RED: Color = Color::srgb(1.0, 0.102, 0.169);

    // --- BLACKS ---
    /// Deep Pitch Black / Charcoal for base surfaces (#0D0D11)
    pub const P5_BLACK: Color = Color::srgb(0.051, 0.051, 0.067);

    /// Slightly elevated surface black (#16161D)
    pub const P5_OFF_BLACK: Color = Color::srgb(0.086, 0.086, 0.114);

    /// Charcoal container background (#1E1E26)
    pub const P5_CHARCOAL: Color = Color::srgb(0.118, 0.118, 0.149);

    // --- WHITES ---
    /// Persona 5 Pure Stark White (#FFFFFF)
    pub const P5_WHITE: Color = Color::srgb(1.0, 1.0, 1.0);

    /// Off-White / Light Silver for subtle contrast (#ECECF0)
    pub const P5_OFF_WHITE: Color = Color::srgb(0.925, 0.925, 0.941);

    // --- GREYS ---
    /// Light Silver Grey for secondary labels (#C7C7D0)
    pub const P5_LIGHT_GREY: Color = Color::srgb(0.78, 0.78, 0.816);

    /// Mid Grey for borders and structural elements (#70707D)
    pub const P5_GREY: Color = Color::srgb(0.439, 0.439, 0.490);

    /// Subtle muted text color (#8E8E9B)
    pub const P5_MUTED: Color = Color::srgb(0.557, 0.557, 0.608);

    /// Dark border line (#353542)
    pub const P5_BORDER: Color = Color::srgb(0.208, 0.208, 0.259);

    // --- CODE STYLING (Strictly Reds, Whites, Blacks & Greys) ---
    pub const CODE_BG: Color = Color::srgb(0.035, 0.035, 0.047);
    pub const CODE_HEADER_BG: Color = Color::srgb(0.10, 0.10, 0.13);
    pub const CODE_KEYWORD: Color = Color::srgb(0.95, 0.12, 0.20);  // Punchy P5 crimson
    pub const CODE_TYPE: Color = Color::srgb(1.0, 1.0, 1.0);        // Pure stark white
    pub const CODE_FUNCTION: Color = Color::srgb(0.88, 0.88, 0.92); // Light silver
    pub const CODE_STRING: Color = Color::srgb(0.75, 0.75, 0.80);   // Silver grey
    pub const CODE_COMMENT: Color = Color::srgb(0.48, 0.48, 0.54);  // Muted slate grey
    pub const CODE_NUMBER: Color = Color::srgb(0.92, 0.92, 0.95);   // Crisp off-white
    pub const CODE_TEXT: Color = Color::srgb(0.98, 0.98, 1.0);      // Crisp white
}

/// Angles and geometry constants for Persona 5 slanted aesthetic
#[allow(dead_code)]
pub mod geometry {
    use bevy::prelude::*;

    /// Primary dynamic banner tilt (~ -3.5 degrees)
    pub const TILT_PRIMARY: f32 = -0.061;

    /// Counter tilt for layered badges (~ +2.2 degrees)
    pub const TILT_COUNTER: f32 = 0.038;

    /// Subtle tilt for smaller tags (~ -1.5 degrees)
    pub const TILT_SUBTLE: f32 = -0.026;

    /// Strong accent tilt (~ -6.0 degrees)
    pub const TILT_STRONG: f32 = -0.105;

    pub fn rot_primary() -> Transform {
        Transform::from_rotation(Quat::from_rotation_z(TILT_PRIMARY))
    }

    pub fn rot_counter() -> Transform {
        Transform::from_rotation(Quat::from_rotation_z(TILT_COUNTER))
    }

    pub fn rot_subtle() -> Transform {
        Transform::from_rotation(Quat::from_rotation_z(TILT_SUBTLE))
    }

    pub fn rot_strong() -> Transform {
        Transform::from_rotation(Quat::from_rotation_z(TILT_STRONG))
    }
}
