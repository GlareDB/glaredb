use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use sqlparser::parser::ParserError;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FileType {
    /// Apache Arrow file
    ARROW,
    /// Apache Avro file
    AVRO,
    /// Apache Parquet file
    PARQUET,
    /// CSV file
    CSV,
    /// JSON file
    JSON,
}

impl FromStr for FileType {
    type Err = ParserError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_uppercase();
        match s.as_str() {
            "ARROW" => Ok(FileType::ARROW),
            "AVRO" => Ok(FileType::AVRO),
            "PARQUET" => Ok(FileType::PARQUET),
            "CSV" => Ok(FileType::CSV),
            "JSON" | "NDJSON" => Ok(FileType::JSON),
            _ => Err(ParserError::ParserError(format!(
                "Unsupported file type {s}"
            ))),
        }
    }
}
/// Readable file compression type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompressionTypeVariant {
    /// Gzip-ed file
    GZIP,
    /// Bzip2-ed file
    BZIP2,
    /// Xz-ed file (liblzma)
    XZ,
    /// Zstd-ed file,
    ZSTD,
    /// Uncompressed file
    UNCOMPRESSED,
}

impl FromStr for CompressionTypeVariant {
    type Err = ParserError;

    fn from_str(s: &str) -> Result<Self, ParserError> {
        let s = s.to_uppercase();
        match s.as_str() {
            "GZIP" | "GZ" => Ok(Self::GZIP),
            "BZIP2" | "BZ2" => Ok(Self::BZIP2),
            "XZ" => Ok(Self::XZ),
            "ZST" | "ZSTD" => Ok(Self::ZSTD),
            "" | "UNCOMPRESSED" => Ok(Self::UNCOMPRESSED),
            _ => Err(ParserError::ParserError(format!(
                "Unsupported file compression type {s}"
            ))),
        }
    }
}

/// Contains the value parsed from Options(...).
///
/// `CREATE ... OPTIONS (abc = 'def')` will return the value
/// `QuotedLiteral("def")` for option "abc" where as `OPTIONS (abc = SECRET
/// def)` will return `Secret(def)` for key "abc".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptionValue {
    QuotedLiteral(String),
    UnquotedLiteral(String),
    Boolean(bool),
    Number(String),
    Secret(String),
}

impl fmt::Display for OptionValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QuotedLiteral(s) => write!(f, "'{s}'"),
            Self::UnquotedLiteral(s) | Self::Number(s) => write!(f, "{s}"),
            Self::Boolean(b) => write!(f, "{}", if *b { "TRUE" } else { "FALSE" }),
            Self::Secret(s) => write!(f, "SECRET {s}"),
        }
    }
}

pub trait ParseOptionValue<T> {
    fn parse_opt(self) -> Result<T, ParserError>;
}

#[macro_export]
macro_rules! parser_err {
    ($($arg:tt)*) => {
        ParserError::ParserError(format!($($arg)*))
    };
}

#[macro_export]
macro_rules! unexpected_type_err {
    ($t:expr, $v:expr) => {
        parser_err!("Expected a {}, got: {}", $t, $v)
    };
}

impl ParseOptionValue<String> for OptionValue {
    fn parse_opt(self) -> Result<String, ParserError> {
        let opt = match self {
            Self::QuotedLiteral(s) | Self::UnquotedLiteral(s) | Self::Number(s) => s,
            Self::Boolean(b) => (if b { "TRUE" } else { "FALSE" }).to_string(),
            o => return Err(unexpected_type_err!("string", o)),
        };
        Ok(opt)
    }
}

impl ParseOptionValue<Vec<String>> for OptionValue {
    fn parse_opt(self) -> Result<Vec<String>, ParserError> {
        match self {
            Self::QuotedLiteral(s) | Self::UnquotedLiteral(s) => {
                Ok(s.split(',').map(|s| s.to_string()).collect())
            }
            o => Err(unexpected_type_err!("string slice", o)),
        }
    }
}

impl ParseOptionValue<bool> for OptionValue {
    fn parse_opt(self) -> Result<bool, ParserError> {
        let opt = match self {
            Self::QuotedLiteral(s) | Self::UnquotedLiteral(s) => match s.as_str() {
                "t" | "true" | "T" | "TRUE" => true,
                "f" | "false" | "F" | "FALSE" => false,
                o => return Err(unexpected_type_err!("boolean", o)),
            },
            Self::Number(n) => {
                let n: u128 = n.parse().map_err(|e| parser_err!("{e}"))?;
                n != 0
            }
            Self::Boolean(b) => b,
            o => return Err(unexpected_type_err!("boolean", o)),
        };
        Ok(opt)
    }
}

impl ParseOptionValue<u16> for OptionValue {
    fn parse_opt(self) -> Result<u16, ParserError> {
        let opt = match self {
            Self::QuotedLiteral(s) | Self::UnquotedLiteral(s) | Self::Number(s) => {
                s.parse().map_err(|e| parser_err!("{e}"))?
            }
            Self::Boolean(b) => {
                if b {
                    1
                } else {
                    0
                }
            }
            o => return Err(unexpected_type_err!("unsigned int", o)),
        };
        Ok(opt)
    }
}

impl ParseOptionValue<usize> for OptionValue {
    fn parse_opt(self) -> Result<usize, ParserError> {
        let opt = match self {
            Self::QuotedLiteral(s) | Self::UnquotedLiteral(s) | Self::Number(s) => {
                s.parse().map_err(|e| parser_err!("{e}"))?
            }
            Self::Boolean(b) => {
                if b {
                    1
                } else {
                    0
                }
            }
            o => return Err(unexpected_type_err!("unsigned int", o)),
        };
        Ok(opt)
    }
}

impl ParseOptionValue<char> for OptionValue {
    fn parse_opt(self) -> Result<char, ParserError> {
        let opt = match self {
            Self::QuotedLiteral(s) | Self::UnquotedLiteral(s) | Self::Number(s) => {
                if s.len() != 1 {
                    return Err(unexpected_type_err!("char", s));
                }
                s.chars().next().unwrap()
            }
            Self::Boolean(b) => {
                if b {
                    't'
                } else {
                    'f'
                }
            }
            o => return Err(unexpected_type_err!("char", o)),
        };
        Ok(opt)
    }
}

impl ParseOptionValue<FileType> for OptionValue {
    fn parse_opt(self) -> Result<FileType, ParserError> {
        let opt = match self {
            Self::QuotedLiteral(s) | Self::UnquotedLiteral(s) => {
                s.parse().map_err(|e| parser_err!("{e}"))?
            }
            o => return Err(unexpected_type_err!("file type", o)),
        };
        Ok(opt)
    }
}

impl ParseOptionValue<CompressionTypeVariant> for OptionValue {
    fn parse_opt(self) -> Result<CompressionTypeVariant, ParserError> {
        let opt = match self {
            Self::QuotedLiteral(s) | Self::UnquotedLiteral(s) => {
                s.parse().map_err(|e| parser_err!("{e}"))?
            }
            o => return Err(unexpected_type_err!("file compression type", o)),
        };
        Ok(opt)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementOptions {
    m: BTreeMap<String, OptionValue>,
}

impl fmt::Display for StatementOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OPTIONS (")?;
        let mut sep = "";
        for (k, v) in self.m.iter() {
            write!(f, "{sep}{k} = {v}")?;
            sep = ", ";
        }
        write!(f, ")")
    }
}

// impl TryFrom<&mut StatementOptions> for StorageOptions {
//     type Error = ParserError;

//     fn try_from(value: &mut StatementOptions) -> Result<Self, Self::Error> {
//         let mut inner = BTreeMap::new();
//         for (key, value) in value.m.iter() {
//             inner.insert(key.clone(), value.clone().parse_opt()?);
//         }
//         Ok(StorageOptions { inner })
//     }
// }

impl StatementOptions {
    pub fn new(m: BTreeMap<String, OptionValue>) -> Self {
        Self { m }
    }

    pub fn len(&self) -> usize {
        self.m.len()
    }

    pub fn is_empty(&self) -> bool {
        self.m.is_empty()
    }

    pub fn remove_optional<T>(&mut self, k: &str) -> Result<Option<T>, ParserError>
    where
        OptionValue: ParseOptionValue<T>,
    {
        let val = match self.m.remove(k) {
            Some(v) => v,
            None => return Ok(None),
        };

        fn get_env(k: &str, upper: bool) -> Result<String, ParserError> {
            let key = format!("glaredb_secret_{k}");
            let key = if upper {
                key.to_uppercase()
            } else {
                key.to_lowercase()
            };
            std::env::var(key).map_err(|_e| parser_err!("invalid secret '{k}'"))
        }

        let opt = match val {
            OptionValue::Secret(s) => {
                let opt = if let Ok(opt) = get_env(&s, /* uppercase: */ true) {
                    opt
                } else {
                    get_env(&s, /* uppercase: */ false)?
                };
                OptionValue::QuotedLiteral(opt)
            }
            opt => opt,
        };

        let opt = opt.parse_opt()?;
        Ok(Some(opt))
    }

    pub fn remove_optional_or<T>(
        &mut self,
        k: &str,
        or: Option<T>,
    ) -> Result<Option<T>, ParserError>
    where
        OptionValue: ParseOptionValue<T>,
    {
        Ok(self.remove_optional(k)?.or(or))
    }

    pub fn remove_required<T>(&mut self, k: &str) -> Result<T, ParserError>
    where
        OptionValue: ParseOptionValue<T>,
    {
        self.remove_optional(k)?
            .ok_or(parser_err!("missing option: {k}"))
    }

    pub fn remove_required_or<T>(&mut self, k: &str, or: Option<T>) -> Result<T, ParserError>
    where
        OptionValue: ParseOptionValue<T>,
    {
        self.remove_optional_or(k, or)?
            .ok_or(parser_err!("missing option: {k}"))
    }
}
