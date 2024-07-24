use std::mem::transmute;

use futures::future::BoxFuture;

/// Transmutes the future into a future with static lifetime bounds.
///
/// This lets us store boxed futures on partition/operator states without having
/// to make everything resort to static lifetime.
///
/// The use case is around COPY TO, INSERTs, and other "extension" points where
/// we're interacting with the outside world. The idea is we want
/// implementations for those extension points to just be async functions.
/// However we don't want those async functions to require 'static lifetime
/// bounds as that would make them incredibly difficult to use. That is where
/// this function comes in handy.
///
/// Using this function requires:
///
/// - The object that creates the future outlives the future itself.
/// - Traits that produce futures where this function is useful must create
///   futures that match the lifetime of the object creating them (e.g.
///   CopyToSink).
/// - An in-code comment explaining rationale for using this function.
///
/// It _might_ be possible to remove this by threading down a 'query or
/// 'pipeline lifetime through all of the execution layer, but I don't know if
/// that will work, nor if it's worth the time right now.
///
/// See:
///
/// - <https://internals.rust-lang.org/t/is-it-ever-legal-to-transmute-a-t-to-a-longer-lifetime/19915/7>
/// - <https://github.com/crossbeam-rs/crossbeam/blob/2a82b619bef638f328776714ec7ccf022859dda2/crossbeam-utils/src/thread.rs#L464-L467>
pub unsafe fn make_static<T>(fut: BoxFuture<'_, T>) -> BoxFuture<'static, T> {
    transmute(fut)
}
