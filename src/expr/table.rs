use std::borrow::Cow;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableReference<'a> {
    Unqualified {
        table: Cow<'a, str>,
    },
    PartiallyQualified {
        schema: Cow<'a, str>,
        table: Cow<'a, str>,
    },
    FullyQualified {
        catalog: Cow<'a, str>,
        schema: Cow<'a, str>,
        table: Cow<'a, str>,
    },
}

pub type OwnedTableReference = TableReference<'static>;

impl<'a> fmt::Display for TableReference<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unqualified { table } => write!(f, "{table}"),
            Self::PartiallyQualified { schema, table } => write!(f, "{schema}.{table}"),
            Self::FullyQualified {
                catalog,
                schema,
                table,
            } => write!(f, "{catalog}.{schema}.{table}"),
        }
    }
}
