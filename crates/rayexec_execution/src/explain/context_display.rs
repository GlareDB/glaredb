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

impl<'a> From<&'a BindContext> for ContextDisplayMode<'a> {
    fn from(context: &'a BindContext) -> Self {
        ContextDisplayMode::Enriched(context)
    }
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
    pub fn with_mode(item: D, mode: impl Into<ContextDisplayMode<'a>>) -> Self {
        ContextDisplayWrapper {
            mode: mode.into(),
            item,
        }
    }
}

impl<D: ContextDisplay> fmt::Display for ContextDisplayWrapper<'_, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.item.fmt_using_context(self.mode, f)
    }
}
