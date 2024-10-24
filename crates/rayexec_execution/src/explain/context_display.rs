use std::fmt;

use crate::logical::binder::bind_context::BindContext;

/// Prints out `val` using the context display mode.
///
/// Should only be used for debugging.
pub fn debug_print_context(mode: ContextDisplayMode, val: &impl ContextDisplay) {
    let wrapper = ContextDisplayWrapper { mode, item: val };
    println!("{wrapper}");
}

#[derive(Debug, Clone, Copy)]
pub enum ContextDisplayMode<'a> {
    Enriched(&'a BindContext),
    Raw,
}

pub trait ContextDisplay {
    /// Format self by enriching the string output with information in the bind
    /// context if it's provided.
    ///
    /// This is primarily to get the original column/tables names from their
    /// numeric references. Expressions should implement this such that the
    /// explain output contains human readable columns, and not just the
    /// references.
    fn fmt_using_context(
        &self,
        mode: ContextDisplayMode,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result;
}

/// Auto-implement for references.
impl<D: ContextDisplay> ContextDisplay for &D {
    fn fmt_using_context(
        &self,
        mode: ContextDisplayMode,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        ContextDisplay::fmt_using_context(*self, mode, f)
    }
}

#[derive(Debug)]
pub struct ContextDisplayWrapper<'a, D> {
    pub mode: ContextDisplayMode<'a>,
    pub item: D,
}

impl<'a, D> ContextDisplayWrapper<'a, D> {
    pub fn with_mode(item: D, mode: ContextDisplayMode<'a>) -> Self {
        ContextDisplayWrapper { mode, item }
    }
}

impl<'a, D: ContextDisplay> fmt::Display for ContextDisplayWrapper<'a, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.item.fmt_using_context(self.mode, f)
    }
}
