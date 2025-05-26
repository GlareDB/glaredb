use std::borrow::Cow;
use std::fmt;

/// A single test or benchmark.
///
/// The original `libtest` often calls benchmarks "tests", which is a bit
/// confusing. So in this library, it is called "trial".
///
/// A trial is created via `Trial::test` or `Trial::bench`. The trial's `name`
/// is printed and used for filtering. The `runner` is called when the
/// test/benchmark is executed to determine its outcome. If `runner` panics, the
/// trial is considered "failed". If you need the behavior of `#[should_panic]`
/// you need to catch the panic yourself. You likely want to compare the panic
/// payload to an expected value anyway.
pub struct Trial {
    pub(crate) runner: Box<dyn FnOnce(bool) -> Outcome + Send>,
    pub(crate) info: TestInfo,
}

impl Trial {
    /// Creates a (non-benchmark) test with the given name and runner.
    ///
    /// The runner returning `Ok(())` is interpreted as the test passing. If the
    /// runner returns `Err(_)`, the test is considered failed.
    pub fn test<R>(name: impl Into<String>, runner: R) -> Self
    where
        R: FnOnce() -> Result<(), Failed> + Send + 'static,
    {
        Self {
            runner: Box::new(move |_test_mode| match runner() {
                Ok(()) => Outcome::Passed,
                Err(failed) => Outcome::Failed(failed),
            }),
            info: TestInfo {
                name: name.into(),
                kind: String::new(),
                is_ignored: false,
                is_bench: false,
            },
        }
    }

    /// Creates a benchmark with the given name and runner.
    ///
    /// If the runner's parameter `test_mode` is `true`, the runner function
    /// should run all code just once, without measuring, just to make sure it
    /// does not panic. If the parameter is `false`, it should perform the
    /// actual benchmark. If `test_mode` is `true` you may return `Ok(None)`,
    /// but if it's `false`, you have to return a `Measurement`, or else the
    /// benchmark is considered a failure.
    ///
    /// `test_mode` is `true` if neither `--bench` nor `--test` are set, and
    /// `false` when `--bench` is set. If `--test` is set, benchmarks are not
    /// ran at all, and both flags cannot be set at the same time.
    pub fn bench<R>(name: impl Into<String>, runner: R) -> Self
    where
        R: FnOnce(bool) -> Result<Option<Measurement>, Failed> + Send + 'static,
    {
        Self {
            runner: Box::new(move |test_mode| match runner(test_mode) {
                Err(failed) => Outcome::Failed(failed),
                Ok(_) if test_mode => Outcome::Passed,
                Ok(Some(measurement)) => Outcome::Measured(measurement),
                Ok(None) => {
                    Outcome::Failed("bench runner returned `Ok(None)` in bench mode".into())
                }
            }),
            info: TestInfo {
                name: name.into(),
                kind: String::new(),
                is_ignored: false,
                is_bench: true,
            },
        }
    }

    /// Sets the "kind" of this test/benchmark. If this string is not
    /// empty, it is printed in brackets before the test name (e.g.
    /// `test [my-kind] test_name`). (Default: *empty*)
    ///
    /// This is the only extension to the original libtest.
    pub fn with_kind(self, kind: impl Into<String>) -> Self {
        Self {
            info: TestInfo {
                kind: kind.into(),
                ..self.info
            },
            ..self
        }
    }

    /// Sets whether or not this test is considered "ignored". (Default: `false`)
    ///
    /// With the built-in test suite, you can annotate `#[ignore]` on tests to
    /// not execute them by default (for example because they take a long time
    /// or require a special environment). If the `--ignored` flag is set,
    /// ignored tests are executed, too.
    pub fn with_ignored_flag(self, is_ignored: bool) -> Self {
        Self {
            info: TestInfo {
                is_ignored,
                ..self.info
            },
            ..self
        }
    }

    /// Returns the name of this trial.
    pub fn name(&self) -> &str {
        &self.info.name
    }

    /// Returns the kind of this trial. If you have not set a kind, this is an
    /// empty string.
    pub fn kind(&self) -> &str {
        &self.info.kind
    }

    /// Returns whether this trial has been marked as *ignored*.
    pub fn has_ignored_flag(&self) -> bool {
        self.info.is_ignored
    }

    /// Returns `true` iff this trial is a test (as opposed to a benchmark).
    pub fn is_test(&self) -> bool {
        !self.info.is_bench
    }

    /// Returns `true` iff this trial is a benchmark (as opposed to a test).
    pub fn is_bench(&self) -> bool {
        self.info.is_bench
    }
}

impl fmt::Debug for Trial {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct OpaqueRunner;
        impl fmt::Debug for OpaqueRunner {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("<runner>")
            }
        }

        f.debug_struct("Test")
            .field("runner", &OpaqueRunner)
            .field("name", &self.info.name)
            .field("kind", &self.info.kind)
            .field("is_ignored", &self.info.is_ignored)
            .field("is_bench", &self.info.is_bench)
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct TestInfo {
    pub name: String,
    pub kind: String,
    pub is_ignored: bool,
    pub is_bench: bool,
}

impl TestInfo {
    pub fn test_name_with_kind(&self) -> Cow<'_, str> {
        if self.kind.is_empty() {
            Cow::Borrowed(&self.name)
        } else {
            Cow::Owned(format!("[{}] {}", self.kind, self.name))
        }
    }
}

/// Output of a benchmark.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Measurement {
    /// Average time in ns.
    pub avg: u64,

    /// Variance in ns.
    pub variance: u64,
}

/// Indicates that a test/benchmark has failed. Optionally carries a message.
///
/// You usually want to use the `From` impl of this type, which allows you to
/// convert any `T: fmt::Display` (e.g. `String`, `&str`, ...) into `Failed`.
#[derive(Debug, Clone)]
pub struct Failed {
    pub(crate) msg: Option<String>,
}

impl Failed {
    /// Creates an instance without message.
    pub fn without_message() -> Self {
        Self { msg: None }
    }

    /// Returns the message of this instance.
    pub fn message(&self) -> Option<&str> {
        self.msg.as_deref()
    }
}

impl<M: std::fmt::Display> From<M> for Failed {
    fn from(msg: M) -> Self {
        Self {
            msg: Some(msg.to_string()),
        }
    }
}

/// The outcome of performing a test/benchmark.
#[derive(Debug, Clone)]
pub(crate) enum Outcome {
    /// The test passed.
    Passed,
    /// The test or benchmark failed.
    Failed(Failed),
    /// The test or benchmark was ignored.
    Ignored,
    /// The benchmark was successfully run.
    Measured(Measurement),
}
