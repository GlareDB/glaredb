/// A tiny HyperLogLog sketch over out 64-bit hashes.
#[derive(Debug)]
pub struct HyperLogLog {
    p: u8,
    m: usize,
    registers: Vec<u8>, // Each register holds a 6-bit value.
}

impl HyperLogLog {
    /// p: 12
    /// m: 4096
    /// Memory: 4KB
    /// Error: 1.6%
    pub const DEFAULT_P: u8 = 12;

    /// Create a new HLL with a given precision (4 <= p <= 16).
    ///
    /// Higher `p` => more registers => lower error => more memory.
    pub fn new(p: u8) -> Self {
        assert!(
            (4..=16).contains(&p),
            "precision p must be between 4 and 16"
        );
        let m = 1_usize << p;
        HyperLogLog {
            p,
            m,
            registers: vec![0; m],
        }
    }

    /// Insert a 64-bit hash into the sketch.
    pub fn insert(&mut self, hash: u64) {
        // Take the top `p` bits as register index
        let idx = (hash >> (64 - self.p)) as usize;
        // The remaining bits...
        let w = (hash << self.p) | (1 << (self.p - 1));
        // Count leading zeros in the **remaining** 64-p bits, plus 1
        let rank = w.leading_zeros() + 1;
        let rank = rank as u8;
        // Update the register with max rankk seen so far
        if self.registers[idx] < rank {
            self.registers[idx] = rank;
        }
    }

    /// Merge another sketch into this one (in-place union).
    pub fn merge(&mut self, other: &HyperLogLog) {
        assert_eq!(self.p, other.p, "precisions must match");
        for (a, &b) in self.registers.iter_mut().zip(&other.registers) {
            if *a < b {
                *a = b;
            }
        }
    }

    /// Estimate the cardinality.
    pub fn count(&self) -> f64 {
        let m = self.m as f64;
        // alpha_m constant depends on m
        //
        // Values taken from wikipedia:
        // <https://en.wikipedia.org/wiki/HyperLogLog>
        let alpha = match self.m {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / m),
        };

        // Harmonic mean of 2^{-register}
        let sum: f64 = self
            .registers
            .iter()
            .map(|&r| 2_f64.powi(-(r as i32)))
            .sum();

        let e = alpha * m * m / sum;

        // Small‐range correction (linear counting) if needed
        if e <= 5.0 * m {
            let zeros = self.registers.iter().filter(|&&r| r == 0).count() as f64;
            if zeros > 0.0 {
                // Linear counting
                m * (m / zeros).ln()
            } else {
                e
            }
        } else if e <= (1u64 << 32) as f64 / 30.0 {
            // Intermediate range, no correction
            e
        } else {
            // Large‐range correction
            -((1u64 << 32) as f64) * (1.0 - e / (1u64 << 32) as f64).ln()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hll_basic() {
        let mut h1 = HyperLogLog::new(10); // 1024 registers
        let mut h2 = HyperLogLog::new(10);

        // Insert 10_000 distinct hashes into both
        for i in 0..10_000 {
            h1.insert(hash64(i));
            h2.insert(hash64(i + 5_000)); // overlap of 5_000
        }

        // h1 estimates ~10_000
        let c1 = h1.count();
        assert!((9000.0..11000.0).contains(&c1), "c1: {c1}");

        // h2 estimates ~10_000
        let c2 = h2.count();
        assert!((9000.0..11000.0).contains(&c2), "c2: {c2}");

        // union
        h1.merge(&h2);
        // true union size = 15_000
        let cu = h1.count();
        assert!((14000.0..16000.0).contains(&cu), "cu: {cu}");
    }

    // Simple (portable) hash for testing.
    fn hash64(x: u64) -> u64 {
        // Use a simple splitmix64
        // <https://rosettacode.org/wiki/Pseudo-random_numbers/Splitmix64>
        let mut z = x.wrapping_add(0x9e3779b97f4a7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }
}
