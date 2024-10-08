#![allow(dead_code)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

pub static GLOBAL_JITTER: Jitter<10> = Jitter {
    timings: [1, 2, 256, 4, 128, 6, 64, 8, 32, 16],
    next: AtomicUsize::new(0),
};

#[derive(Debug)]
pub struct Jitter<const N: usize> {
    timings: [u64; N],
    next: AtomicUsize,
}

impl<const N: usize> Jitter<N> {
    pub fn sleep_jitter(&self) {
        let idx = self.next.fetch_add(1, Ordering::Relaxed);
        let ms = self.timings[idx % N];
        std::thread::sleep(Duration::from_millis(ms));
    }
}
