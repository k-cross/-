use bevy::prelude::*;
use bevy::asset::AssetId;

const IMPACT_BYTES: &[u8] = include_bytes!("../../assets/fonts/impact.ttf");
const ARIAL_BOLD_BYTES: &[u8] = include_bytes!("../../assets/fonts/arial_bold.ttf");
const ARIAL_BLACK_BYTES: &[u8] = include_bytes!("../../assets/fonts/arial_black.ttf");
const GEORGIA_BOLD_BYTES: &[u8] = include_bytes!("../../assets/fonts/georgia_bold.ttf");
const COURIER_BOLD_BYTES: &[u8] = include_bytes!("../../assets/fonts/courier_bold.ttf");
const SYMBOLS_BYTES: &[u8] = include_bytes!("../../assets/fonts/symbols.ttf");

/// Centralized typographic font assets for the entire deck.
/// Provides authentic Persona 5 graphic typography and full symbol coverage.
#[derive(Resource, Clone)]
pub struct FontAssets {
    /// Heavy condensed comic display font for title banners (Impact)
    pub display: Handle<Font>,
    /// Clean bold modern geometric sans-serif for menus, HUD, cards (Arial Bold)
    pub sans: Handle<Font>,
    /// Ultra-heavy punchy sans-serif for loud tags, badges, and accents (Arial Black)
    pub sans_heavy: Handle<Font>,
    /// Sharp editorial serif for ransom cutout letters (Georgia Bold)
    pub serif: Handle<Font>,
    /// Classic typewriter monospace for code and technical stamps (Courier New Bold)
    pub monospace: Handle<Font>,
    /// Comprehensive vector symbol font for stars (★), arrows (▶), bullets (•), and meter blocks (▮, ▯)
    pub symbols: Handle<Font>,
}

impl FontAssets {
    /// Selects a font family from the cutout rotation palette based on character index.
    /// Provides authentic magazine-cutout / ransom-note typography variety.
    #[allow(dead_code)]
    pub fn cutout_font(&self, index: usize) -> Handle<Font> {
        match index % 5 {
            0 => self.display.clone(),
            1 => self.sans_heavy.clone(),
            2 => self.serif.clone(),
            3 => self.monospace.clone(),
            _ => self.sans.clone(),
        }
    }
}

impl FromWorld for FontAssets {
    fn from_world(world: &mut World) -> Self {
        let mut fonts = world.resource_mut::<Assets<Font>>();
        let display = fonts.add(Font::from_bytes(IMPACT_BYTES.to_vec()));
        let sans = fonts.add(Font::from_bytes(ARIAL_BOLD_BYTES.to_vec()));
        let sans_heavy = fonts.add(Font::from_bytes(ARIAL_BLACK_BYTES.to_vec()));
        let serif = fonts.add(Font::from_bytes(GEORGIA_BOLD_BYTES.to_vec()));
        let monospace = fonts.add(Font::from_bytes(COURIER_BOLD_BYTES.to_vec()));
        let symbols = fonts.add(Font::from_bytes(SYMBOLS_BYTES.to_vec()));

        // Also upgrade the engine's default font (AssetId::default()) from FiraMono-subset
        // to Arial Bold so any unadorned TextFont automatically inherits clean, bold sans-serif.
        let _ = fonts.insert(AssetId::default(), Font::from_bytes(ARIAL_BOLD_BYTES.to_vec()));

        FontAssets {
            display,
            sans,
            sans_heavy,
            serif,
            monospace,
            symbols,
        }
    }
}

pub struct FontPlugin;

impl Plugin for FontPlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<FontAssets>();
    }
}
