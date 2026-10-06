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
    pub const CODE_KEYWORD: Color = Color::srgb(0.95, 0.12, 0.20); // Punchy P5 crimson
    pub const CODE_TYPE: Color = Color::srgb(1.0, 1.0, 1.0); // Pure stark white
    pub const CODE_FUNCTION: Color = Color::srgb(0.88, 0.88, 0.92); // Light silver
    pub const CODE_STRING: Color = Color::srgb(0.75, 0.75, 0.80); // Silver grey
    pub const CODE_COMMENT: Color = Color::srgb(0.48, 0.48, 0.54); // Muted slate grey
    pub const CODE_NUMBER: Color = Color::srgb(0.92, 0.92, 0.95); // Crisp off-white
    pub const CODE_TEXT: Color = Color::srgb(0.98, 0.98, 1.0); // Crisp white

    // --- P5 ACCENT HIGHLIGHTS (Shop & Menu Special Selections) ---
    /// Vibrant Gold / Amber accent (e.g. Iwai Airsoft Shop SELL card)
    pub const P5_GOLD: Color = Color::srgb(1.0, 0.78, 0.0);
    /// Prismatic Neon Cyan slice accent (Reference 1 ITEM wedge)
    pub const P5_CYAN: Color = Color::srgb(0.0, 0.90, 1.0);
    /// Prismatic Neon Magenta slice accent (Reference 1 ITEM wedge)
    pub const P5_MAGENTA: Color = Color::srgb(1.0, 0.0, 0.5);
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

    pub fn rot_primary() -> UiTransform {
        UiTransform::from_rotation(Rot2::radians(TILT_PRIMARY))
    }

    pub fn rot_counter() -> UiTransform {
        UiTransform::from_rotation(Rot2::radians(TILT_COUNTER))
    }

    pub fn rot_subtle() -> UiTransform {
        UiTransform::from_rotation(Rot2::radians(TILT_SUBTLE))
    }
}

/// Procedural organic ink blotch and high-velocity directional spray constants.
/// Ink pools break straight lines with multi-lobed organic geometry and
/// radiating satellite droplets.
#[allow(dead_code)]
pub mod ink {
    /// Large ink pool (px)
    pub const RADIUS_LG: f32 = 85.0;
    /// Extra-large ink pool anchoring major title areas (px)
    pub const RADIUS_XL: f32 = 110.0;

    // --- Nucleus (central pooling) ellipse aspect ratios ---
    /// Primary nucleus: wide ellipse (width × height multipliers relative to radius)
    pub const NUCLEUS_A: (f32, f32) = (1.9, 1.4);
    /// Secondary nucleus: tall ellipse
    pub const NUCLEUS_B: (f32, f32) = (1.5, 1.8);
    /// Tertiary nucleus: diamond-point square
    pub const NUCLEUS_C: (f32, f32) = (1.2, 1.2);

    /// Rotation angle of the nucleus layers (radians)
    pub const NUCLEUS_ROT_A: f32 = 0.35;
    pub const NUCLEUS_ROT_B: f32 = -0.45;
    /// 45° diamond-point rotation for tertiary nucleus
    pub const NUCLEUS_ROT_C: f32 = 0.78;

    /// Nucleus B offset multipliers (relative to radius)
    pub const NUCLEUS_B_OFFSET: (f32, f32) = (0.15, -0.1);
    /// Nucleus C offset multipliers (relative to radius)
    pub const NUCLEUS_C_OFFSET: (f32, f32) = (-0.2, 0.15);

    /// Z-layer for satellite droplets (above nucleus)
    pub const DROPLET_Z: f32 = 0.3;
}

/// Explosive comic action starburst and wax seal stamp geometry.
/// Jagged multi-point bubbles and calling card seals.
#[allow(dead_code)]
pub mod starburst {
    use bevy::prelude::*;

    /// Standard comic starburst tilt (~+5.7°)
    pub const STARBURST_TILT: f32 = 0.10;

    /// Asymmetric starburst border: thick left + bottom-right bulge.
    /// Pattern: (left: 3.5, top: 1.5, right: 4.5, bottom: 4.5)
    pub fn starburst_border() -> UiRect {
        UiRect {
            left: Val::Px(3.5),
            top: Val::Px(1.5),
            right: Val::Px(4.5),
            bottom: Val::Px(4.5),
        }
    }

    /// Wax seal tilt (~+6.9°)
    pub const SEAL_TILT: f32 = 0.12;

    /// Wax seal asymmetric border: thin top-left, heavy bottom-right.
    /// Pattern: (left: 3.0, top: 3.0, right: 6.0, bottom: 6.0)
    pub fn seal_border() -> UiRect {
        UiRect {
            left: Val::Px(3.0),
            top: Val::Px(3.0),
            right: Val::Px(6.0),
            bottom: Val::Px(6.0),
        }
    }

    /// Wax seal padding (horizontal, vertical) in px
    pub const SEAL_PAD: (f32, f32) = (18.0, 8.0);

    /// Starburst padding (horizontal, vertical) in px
    pub const STARBURST_PAD: (f32, f32) = (16.0, 5.0);
}

/// Captivity motif constants: diagonal hazard stripes, shattered chain links,
/// and razor crimson fracture lines evoking prison / confinement.
#[allow(dead_code)]
pub mod captivity {
    /// Individual hazard stripe bar width (px)
    pub const STRIPE_WIDTH: f32 = 20.0;
    /// Individual hazard stripe bar height (px)
    pub const STRIPE_HEIGHT: f32 = 22.0;
    /// Spacing between stripe centers (px)
    pub const STRIPE_SPACING: f32 = 44.0;
    /// Diagonal angle for hazard stripe ribbon (~-20°)
    pub const STRIPE_ANGLE: f32 = -0.35;
    /// Total number of stripe bars in the ribbon
    pub const STRIPE_COUNT: usize = 40;

    // --- Shattered chain link dimensions ---
    /// Outer link bounding rect (width × height px)
    pub const CHAIN_WIDTH: f32 = 38.0;
    pub const CHAIN_HEIGHT: f32 = 18.0;
    /// Inner cutout hole (width × height px)
    pub const CHAIN_CUTOUT_WIDTH: f32 = 24.0;
    pub const CHAIN_CUTOUT_HEIGHT: f32 = 8.0;
    /// Razor crimson fracture break line width (px)
    pub const FRACTURE_WIDTH: f32 = 5.0;
    /// Fracture break line height (matches chain outer height)
    pub const FRACTURE_HEIGHT: f32 = 18.0;
    /// Fracture line x-offset from chain center
    pub const FRACTURE_OFFSET_X: f32 = 12.0;
    /// Fracture line rotation (radians)
    pub const FRACTURE_ANGLE: f32 = 0.3;
}

/// Kinetic motion engine timing constants: slam entrances, punk jitter,
/// bobbing cursors, and razor screen slash blade wipes.
#[allow(dead_code)]
pub mod motion {
    // --- SlamEntrance (violent 15-frame slam-in) ---
    /// Duration of the slam entrance animation (seconds, ~17 frames at 60fps)
    pub const SLAM_DURATION: f32 = 0.28;
    /// Initial scale factor before slam burst (0.4 = compact, bursts to 1.0)
    pub const SLAM_INITIAL_SCALE: f32 = 0.4;
    /// ease_out_back overshoot coefficient (snappy spring bounce)
    pub const SLAM_OVERSHOOT: f32 = 1.70158;

    // --- ScreenSlashBlade (razor diagonal screen wipe) ---
    /// Duration for the heavy black cut blade (seconds)
    pub const SLASH_BLADE_DURATION: f32 = 0.22;
    /// Duration for the crimson razor edge (seconds)
    pub const SLASH_EDGE_DURATION: f32 = 0.20;
    /// Duration for the white fracture line (seconds)
    pub const SLASH_FRACTURE_DURATION: f32 = 0.18;
    /// Diagonal slash angle for all blade layers (~-23°)
    pub const SLASH_BLADE_ANGLE: f32 = -0.40;

    // --- Character slash/lunge ---
    /// Phantom Thief forward slash lunge duration (seconds)
    pub const CHARACTER_SLASH_DURATION: f32 = 0.35;
}

/// Consolidated typographic scale extracted across all slides, HUD, and
/// engine components. Provides a consistent type ramp for the entire deck.
#[allow(dead_code)]
pub mod typography {
    // --- Heading sizes ---
    /// Extra-large slide title (px)
    pub const FONT_HEADING_XL: f32 = 36.0;

    // --- Body sizes ---
    /// Primary body text / subtitle cards (px)
    pub const FONT_BODY: f32 = 15.0;
    /// Card header / callout title (px)
    pub const FONT_BODY_LG: f32 = 14.5;
    /// Card icon / inline accent (px)
    pub const FONT_BODY_ICON: f32 = 14.0;
    /// Calling card & code block body / key prompts (px)
    pub const FONT_BODY_SM: f32 = 13.0;
    /// Menu items, callout body, sign-off (px)
    pub const FONT_CAPTION: f32 = 12.0;
    /// Menu items slightly larger variant (px)
    pub const FONT_CAPTION_LG: f32 = 12.5;

    // --- Label / tag sizes ---
    /// Muted annotations, HUD labels, calling card body (px)
    pub const FONT_LABEL: f32 = 11.5;
    /// HUD sub-labels, palace security text (px)
    pub const FONT_LABEL_SM: f32 = 11.0;
    /// Smallest tag / stamp annotations (px)
    pub const FONT_TAG_SM: f32 = 10.0;

    // --- Specialized sizes ---
    /// Code block monospace text (px)
    pub const FONT_CODE: f32 = 13.0;
    /// HUD slide counter text (px)
    pub const FONT_HUD_COUNTER: f32 = 15.0;
    /// HUD key prompt text (px)
    pub const FONT_HUD_PROMPT: f32 = 13.0;
    /// HUD star/separator decorative (px)
    pub const FONT_HUD_STAR: f32 = 14.0;
    /// HUD "TAKE YOUR TIME" label (px)
    pub const FONT_HUD_LABEL: f32 = 12.0;
}
