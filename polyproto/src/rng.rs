const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;

#[derive(Debug, Clone)]
pub struct Rng {
    state: [u64; 4],
}

impl Rng {
    fn mix(z: u64) -> u64 {
        let z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        let z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn to_unit(bits: u64) -> f64 {
        (bits >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }

    #[must_use]
    pub fn hashed_unit(key: u64) -> f64 {
        Self::to_unit(Self::mix(key.wrapping_add(GOLDEN)))
    }

    #[must_use]
    pub fn new(seed: u64) -> Self {
        let mut z = seed.wrapping_add(GOLDEN);
        let mut next = || {
            z = z.wrapping_add(GOLDEN);
            Self::mix(z)
        };
        Self {
            state: [next(), next(), next(), next()],
        }
    }

    pub fn next_u64(&mut self) -> u64 {
        let s = &mut self.state;
        let result = s[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = s[1] << 17;
        s[2] ^= s[0];
        s[3] ^= s[1];
        s[1] ^= s[2];
        s[0] ^= s[3];
        s[2] ^= t;
        s[3] = s[3].rotate_left(45);
        result
    }

    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next_u64() % n }
    }

    pub fn unit(&mut self) -> f64 {
        Self::to_unit(self.next_u64())
    }

    pub fn chance(&mut self, p: f64) -> bool {
        self.unit() < p
    }

    pub fn zipf(&mut self, n: u64, skew: f64) -> u64 {
        let u = self.unit();
        let scaled = u.powf(skew) * n as f64;
        (scaled as u64).min(n.saturating_sub(1))
    }
}
