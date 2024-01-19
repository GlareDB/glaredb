use pgrepr::notice::NoticeSeverity;

use super::*;

pub trait Value: ToOwned + std::fmt::Debug {
    fn try_parse(s: &str) -> Option<Self::Owned>;
    fn format(&self) -> String;
}

impl Value for str {
    fn try_parse(s: &str) -> Option<Self::Owned> {
        Some(s.to_string())
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

impl Value for String {
    fn try_parse(s: &str) -> Option<Self::Owned> {
        Some(s.to_string())
    }

    fn format(&self) -> String {
        self.clone()
    }
}

impl Value for bool {
    fn try_parse(s: &str) -> Option<Self::Owned> {
        match s {
            "t" | "true" => Some(true),
            "f" | "false" => Some(false),
            _ => None,
        }
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

impl Value for i32 {
    fn try_parse(s: &str) -> Option<Self::Owned> {
        s.parse().ok()
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

impl Value for usize {
    fn try_parse(s: &str) -> Option<Self::Owned> {
        s.parse().ok()
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

impl Value for Uuid {
    fn try_parse(s: &str) -> Option<Self::Owned> {
        s.parse().ok()
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

impl<V> Value for Option<V>
where
    V: Value + ?Sized + 'static + ToOwned + Clone + FromStr + Display,
{
    fn try_parse(s: &str) -> Option<Self::Owned> {
        let v = s.parse::<V>().ok()?;
        Some(Some(v))
    }

    fn format(&self) -> String {
        match self {
            Some(v) => v.to_string(),
            None => "None".to_string(),
        }
    }
}

impl Value for [String] {
    fn try_parse(s: &str) -> Option<Self::Owned> {
        Some(split_comma_delimited(s))
    }

    fn format(&self) -> String {
        self.join(",")
    }
}

impl Value for Dialect {
    fn try_parse(s: &str) -> Option<Self::Owned> {
        match s {
            "sql" => Some(Dialect::Sql),
            "prql" => Some(Dialect::Prql),
            _ => None,
        }
    }

    fn format(&self) -> String {
        match self {
            Dialect::Sql => "sql".to_string(),
            Dialect::Prql => "prql".to_string(),
        }
    }
}

impl Value for NoticeSeverity {
    fn try_parse(s: &str) -> Option<NoticeSeverity> {
        NoticeSeverity::from_str(s).ok()
    }

    fn format(&self) -> String {
        self.to_string()
    }
}
