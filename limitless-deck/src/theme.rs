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

/// Per-letter magazine cutout / ransom-note typography construction constants.
/// Each letter sits on an individually clipped scrap with conflicting sizes,
/// contrasting backgrounds, asymmetric borders, and distinct tilt angles.
#[allow(dead_code)]
pub mod cutout {
    use bevy::prelude::*;

    // --- Font size range for cutout letter scraps ---
    /// Minimum cutout letter font size (px)
    pub const FONT_SIZE_MIN: f32 = 28.0;
    /// Maximum cutout letter font size (px)
    pub const FONT_SIZE_MAX: f32 = 52.0;

    // --- Tilt angle range (radians) for letter scraps ---
    /// Maximum counter-clockwise tilt for letter scraps (~-12.6°)
    pub const TILT_MIN: f32 = -0.22;
    /// Maximum clockwise tilt for letter scraps (~+8.0°)
    pub const TILT_MAX: f32 = 0.14;

    // --- Scrap padding ranges (px) ---
    pub const PAD_H_MIN: f32 = 8.0;
    pub const PAD_H_MAX: f32 = 13.0;
    pub const PAD_V_MIN: f32 = 4.0;
    pub const PAD_V_MAX: f32 = 6.0;

    /// Standard inter-letter gap margin (px)
    pub const SCRAP_MARGIN: f32 = 1.5;

    // --- Torn masking tape word-gap scrap dimensions ---
    /// Tape scrap width (px) — used between words
    pub const TAPE_WIDTH: f32 = 20.0;
    /// Tape scrap height (px)
    pub const TAPE_HEIGHT: f32 = 12.0;
    /// Smaller tape variant width (px)
    pub const TAPE_WIDTH_SM: f32 = 18.0;
    /// Smaller tape variant height (px)
    pub const TAPE_HEIGHT_SM: f32 = 10.0;
    /// Standard tape side margin (px)
    pub const TAPE_MARGIN: f32 = 8.0;
    /// Smaller tape variant side margin (px)
    pub const TAPE_MARGIN_SM: f32 = 6.0;

    // --- Border thickness presets ---
    /// Thin hairline border (px)
    pub const BORDER_THIN: f32 = 1.5;
    /// Standard border (px)
    pub const BORDER_STD: f32 = 2.0;
    /// Medium-thick accent border (px)
    pub const BORDER_THICK: f32 = 2.5;
    /// Heavy punk accent border (px)
    pub const BORDER_HEAVY: f32 = 5.0;

    /// Asymmetric drop-shadow border: thin top-left, thick bottom-right.
    /// Used on cutout scraps to create a stamped-on depth effect.
    pub fn drop_shadow_br(thin: f32, thick: f32) -> UiRect {
        UiRect {
            left: Val::Px(thin),
            top: Val::Px(thin),
            right: Val::Px(thick),
            bottom: Val::Px(thick),
        }
    }

    /// Asymmetric drop-shadow border: thick left, thin elsewhere.
    /// Used on sidebar accent tags and classification ribbons.
    pub fn drop_shadow_bl(thick: f32, thin: f32) -> UiRect {
        UiRect {
            left: Val::Px(thick),
            top: Val::Px(thin),
            right: Val::Px(thin),
            bottom: Val::Px(thin),
        }
    }
}

/// Procedural organic ink blotch and high-velocity directional spray constants.
/// Ink pools break straight lines with multi-lobed organic geometry and
/// radiating satellite droplets.
#[allow(dead_code)]
pub mod ink {
    // --- Named radius presets for ink blotches ---
    /// Small ink spray accent (px)
    pub const RADIUS_SM: f32 = 36.0;
    /// Medium ink stain (px)
    pub const RADIUS_MD: f32 = 55.0;
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

    // --- Satellite droplet conventions ---
    /// Total number of radiating satellite droplets per blotch
    pub const DROPLET_COUNT: usize = 10;

    /// Droplet size ratio for nearest satellite (relative to radius)
    pub const DROPLET_RATIO_NEAR: f32 = 0.35;
    /// Droplet size ratio for farthest satellite (relative to radius)
    pub const DROPLET_RATIO_FAR: f32 = 0.12;

    // --- Spray reach multipliers (distance from center, relative to radius) ---
    /// Nearest forward spray distance multiplier
    pub const SPRAY_NEAR: f32 = 1.2;
    /// Farthest forward spray distance multiplier
    pub const SPRAY_FAR: f32 = 2.9;
    /// Backsplash near distance multiplier
    pub const BACKSPLASH_NEAR: f32 = 0.9;
    /// Backsplash far distance multiplier
    pub const BACKSPLASH_FAR: f32 = 1.3;
    /// Lateral spray near distance multiplier
    pub const LATERAL_NEAR: f32 = 1.1;
    /// Lateral spray far distance multiplier
    pub const LATERAL_FAR: f32 = 2.0;

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

    // --- Razor accent line widths ---
    /// Primary crimson slash accent width (px)
    pub const RAZOR_ACCENT_WIDTH: f32 = 7.0;
    /// Secondary crimson hairline width (px)
    pub const RAZOR_HAIRLINE_WIDTH: f32 = 2.5;
    /// Torn-edge tape line width (px)
    pub const TORN_TAPE_WIDTH: f32 = 3.5;
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

    // --- PunkJitter (erratic living micro-jitter & breathing) ---
    /// Base oscillation frequency (Hz) — cranked up for aggressive punk energy
    pub const JITTER_FREQUENCY: f32 = 12.0;
    /// Interval between erratic sharp twitches (seconds) — fires twice as often
    pub const JITTER_TWITCH_INTERVAL: f32 = 0.25;
    /// Twitch decay rate (per-second exponential falloff) — snappier snap-back
    pub const JITTER_TWITCH_DECAY: f32 = 12.0;
    /// Micro-pulse scale ceiling (1.0 + this value) — more visible breathing
    pub const JITTER_PULSE_SCALE: f32 = 0.035;

    // --- BobbingCursor (animated menu dagger/arrow) ---
    /// Bobbing oscillation speed
    pub const BOBBING_SPEED: f32 = 8.0;
    /// Bobbing travel distance (px)
    pub const BOBBING_DISTANCE: f32 = 5.0;

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
    /// Large section header (px)
    pub const FONT_HEADING_LG: f32 = 30.0;

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
    /// Classification tags, tiny badges, weather tags (px)
    pub const FONT_TAG: f32 = 10.5;
    /// Smallest tag / stamp annotations (px)
    pub const FONT_TAG_SM: f32 = 10.0;

    // --- Specialized sizes ---
    /// Code block monospace text (px)
    pub const FONT_CODE: f32 = 13.0;
    /// HUD calendar date stamp (px)
    pub const FONT_HUD_DATE: f32 = 17.0;
    /// HUD slide counter text (px)
    pub const FONT_HUD_COUNTER: f32 = 15.0;
    /// HUD key prompt text (px)
    pub const FONT_HUD_PROMPT: f32 = 13.0;
    /// HUD star/separator decorative (px)
    pub const FONT_HUD_STAR: f32 = 14.0;
    /// HUD "TAKE YOUR TIME" label (px)
    pub const FONT_HUD_LABEL: f32 = 12.0;
}
