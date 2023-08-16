use super::*;

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct CopyTo {
    pub source: DfLogicalPlan,
    pub dest: CopyToDestinationOptions,
    pub format: CopyToFormatOptions,
}

impl std::fmt::Debug for CopyTo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CopyTo")
            .field("source", &self.source.schema())
            .field("dest", &self.dest)
            .field("format", &self.format)
            .finish()
    }
}
