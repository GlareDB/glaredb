use std::fmt::Debug;

/// The valid types are:
/// - 'T' - text, varchar results
/// - 'I' - integers
/// - 'R' - floating point numbers
///
/// Any other types are represented with `?`.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ColumnType {
    Text,
    Integer,
    Float,
    Any,
}

impl ColumnType {
    pub fn from_char(value: char) -> Option<Self> {
        match value {
            'T' => Some(Self::Text),
            'I' => Some(Self::Integer),
            'R' => Some(Self::Float),
            _ => Some(Self::Any),
        }
    }

    pub fn to_char(&self) -> char {
        match self {
            Self::Text => 'T',
            Self::Integer => 'I',
            Self::Float => 'R',
            Self::Any => '?',
        }
    }
}
