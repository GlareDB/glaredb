//! Implemenetation of tdigest for computing approximate quantiles.
//!
//! <https://arxiv.org/abs/1902.04023>

use std::cmp::Ordering;

/// A single centroid in a tdigest. Tracks a weighted cluster of points.
#[derive(Clone, Debug)]
pub struct Centroid {
    pub mean: f64,
    pub weight: f64,
}

#[derive(Clone, Debug)]
pub struct TDigest {
    /// Sorted list of centroids by ascending mean.
    centroids: Vec<Centroid>,
    /// Total weight (total count of points inserted).
    total_weight: f64,
    /// Compression parameter 'delta' (controls number of centroids).
    compression: usize,
}

impl TDigest {
    /// Create a new, empty tdigest with the given compression 'delta'.
    pub fn new(compression: usize) -> Self {
        assert_ne!(0, compression, "Compression cannot be zero");

        TDigest {
            centroids: Vec::new(),
            total_weight: 0.0,
            compression,
        }
    }

    /// Insert a single value `x` into the tdigest.
    ///
    /// This will either merge `x` into the nearest centroid (if size
    /// constraints allow), or create a new centroid at `mean = x, weight = 1`.
    /// If the total number of centroids grows too large (> 20 * compression),
    /// it triggers a compression pass.
    pub fn add(&mut self, x: f64) {
        // If we have no centroids yet, just create the first one.
        if self.centroids.is_empty() {
            self.centroids.push(Centroid {
                mean: x,
                weight: 1.0,
            });
            self.total_weight = 1.0;
            return;
        }

        // Find insertion index.
        let idx = match self.centroids.binary_search_by(|c| {
            if c.mean < x {
                Ordering::Less
            } else if c.mean > x {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }) {
            Ok(i) => i,
            Err(i) => i,
        };

        // Determine the 'best' (closest) centroid among neighbors.
        let mut best = idx;
        // Check left neighbor if it exists.
        if idx > 0 {
            let left = idx - 1;
            let d_left = (self.centroids[left].mean - x).abs();
            let d_curr = if idx < self.centroids.len() {
                (self.centroids[idx].mean - x).abs()
            } else {
                f64::INFINITY
            };
            if d_left <= d_curr {
                best = left;
            }
        }
        // Check right neghbor if it exists.
        if idx < self.centroids.len() {
            let d_right = (self.centroids[idx].mean - x).abs();
            let d_best = (self.centroids[best].mean - x).abs();
            if d_right < d_best {
                best = idx;
            }
        }

        // Compute cumulative weight up to (but not including) centroid 'best'.
        let mut cumulative_w = 0.0;
        for i in 0..best {
            cumulative_w += self.centroids[i].weight;
        }
        let w_best = self.centroids[best].weight;

        // Compute current total weight (before adding x).
        let n = self.total_weight;
        // If total_weight is zero (shouldn't happen here since centroids isn't
        // empty), avoid div by zero.
        let q_best = if n > 0.0 {
            (cumulative_w + w_best * 0.5) / n
        } else {
            0.5
        };

        // Compute size‐constraint for that centroid.
        let delta = self.compression as f64;
        let w_max = if n > 0.0 {
            4.0 * n * q_best * (1.0 - q_best) / delta
        } else {
            // If no existing points, allow merge.
            f64::INFINITY
        };

        // If adding one more to 'best' stays <= w_max, merge. Otherwise create
        // new.
        if w_best + 1.0 <= w_max {
            // Merge!
            let new_weight = w_best + 1.0;
            let new_mean = (self.centroids[best].mean * w_best + x) / new_weight;
            self.centroids[best].mean = new_mean;
            self.centroids[best].weight = new_weight;
        } else {
            // Insert a brand‐new centroid at idx.
            let c = Centroid {
                mean: x,
                weight: 1.0,
            };
            self.centroids.insert(idx, c);
        }

        // Increase total wieght.
        self.total_weight += 1.0;

        // If too many centroids, trigger compression
        let threshold = self.compression * 20;
        if self.centroids.len() > threshold {
            self.compress();
        }
    }

    /// Merge another tdigest into self.
    ///
    /// This concatenates all centroids, sums total weights, then runs one
    /// compression pass to restore the size constraiint invariants.
    pub fn merge(&mut self, other: &TDigest) {
        assert!(
            self.compression == other.compression,
            "Cannot merge tdigest with different compression"
        );
        // Append all centroids from other.
        for c in &other.centroids {
            self.centroids.push(c.clone());
        }
        // Update total weight.
        self.total_weight += other.total_weight;
        // Compress the combined list
        self.compress();
    }

    /// Query the approximate value at some quantile.
    ///
    /// If there are no points (total_weight == 0), returns nan.
    pub fn quantile(&self, q: f64) -> f64 {
        if self.centroids.is_empty() || self.total_weight == 0.0 {
            return f64::NAN;
        }
        let target = q.clamp(0.0, 1.0) * self.total_weight;
        let mut cumulative = 0.0;

        // Walk through centroids in ascending order
        for i in 0..self.centroids.len() {
            let w_i = self.centroids[i].weight;
            let m_i = self.centroids[i].mean;
            if target < cumulative + w_i {
                // Found the centroid containing rank 'target'
                let offset = if w_i > 0.0 {
                    (target - cumulative) / w_i
                } else {
                    0.0
                };
                let next_mean = if i + 1 < self.centroids.len() {
                    self.centroids[i + 1].mean
                } else {
                    m_i
                };
                // Linear interpolate between m_i and next_mean
                return m_i + offset * (next_mean - m_i);
            }
            cumulative += w_i;
        }
        // If q == 1.0 or due to rounding, return the last centroid's mean.
        self.centroids.last().map(|c| c.mean).unwrap_or(f64::NAN)
    }

    /// Run one full compression pass.
    ///
    /// This re-sorts all centroids by mean and then merges neighbors whenever
    /// the merged cluster would still satisfy the size constraint.
    fn compress(&mut self) {
        if self.centroids.is_empty() {
            return;
        }

        // Sort by ascending mean
        self.centroids
            .sort_by(|a, b| a.mean.partial_cmp(&b.mean).unwrap_or(Ordering::Equal));

        // Build a new list of centroids, merging when allowed.
        let mut new_list: Vec<Centroid> = Vec::with_capacity(self.centroids.len());
        let mut running_weight = 0.0;
        let delta = self.compression as f64;
        let n = self.total_weight;

        for c in &self.centroids {
            if new_list.is_empty() {
                // First centroid, just push it
                new_list.push(c.clone());
                running_weight = c.weight;
            } else {
                // Consider merging into the last centroid in new_list
                let last = new_list.len() - 1;
                let w_last = new_list[last].weight;
                let m_last = new_list[last].mean;

                // Compute the 'midpoint quantile' of the last centroid.
                let cum_before_last = running_weight - w_last;
                let q_last = if n > 0.0 {
                    (cum_before_last + w_last * 0.5) / n
                } else {
                    0.5
                };

                let w_max = if n > 0.0 {
                    4.0 * n * q_last * (1.0 - q_last) / delta
                } else {
                    f64::INFINITY
                };

                // If merging c into the last centroid still satisfies w_last + w_c <= w_max, merge.
                if w_last + c.weight <= w_max {
                    let new_w = w_last + c.weight;
                    let new_mean = (m_last * w_last + c.mean * c.weight) / new_w;
                    new_list[last].weight = new_w;
                    new_list[last].mean = new_mean;
                    running_weight += c.weight;
                } else {
                    // Otherwise, new centroid.
                    new_list.push(c.clone());
                    running_weight += c.weight;
                }
            }
        }

        // Replace the old centroid list with the compressed one.
        self.centroids = new_list;
    }

    /// Get the number of centroids currently stored.
    pub fn centroids_count(&self) -> usize {
        self.centroids.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_insert_and_quantile() {
        let mut td = TDigest::new(100);
        let data: Vec<f64> = (0..1000).map(|i| i as f64).collect();
        for &x in &data {
            td.add(x);
        }
        // The 50th percentile should be near 499.5
        let median = td.quantile(0.5);
        assert!(
            (median - 499.5).abs() < 20.0,
            "median estimate was {}, expected ~499.5",
            median
        );
        // The 90th percentile should be near 899.0
        let p90 = td.quantile(0.9);
        assert!(
            (p90 - 899.0).abs() < 20.0,
            "90th percentile was {}, expected ~899.0",
            p90
        );
    }

    #[test]
    fn merge_consistency() {
        let mut td1 = TDigest::new(100);
        let mut td2 = TDigest::new(100);
        for i in 0..500 {
            td1.add(i as f64);
        }
        for i in 500..1000 {
            td2.add(i as f64);
        }
        // Merge into a single digest
        td1.merge(&td2);
        let median = td1.quantile(0.5);
        assert!(
            (median - 499.5).abs() < 20.0,
            "merged median = {}, expected ~499.5",
            median
        );
    }

    #[test]
    fn empty_quantile() {
        let td: TDigest = TDigest::new(50);
        assert!(td.quantile(0.5).is_nan());
    }
}
