use glaredb_execution::runtime::time::RuntimeInstant;

/// Instant implementation that wraps std Instant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeInstant(std::time::Instant);

impl RuntimeInstant for NativeInstant {
    fn now() -> Self {
        NativeInstant(std::time::Instant::now())
    }

    fn duration_since(&self, earlier: Self) -> std::time::Duration {
        self.0.saturating_duration_since(earlier.0)
    }
}
