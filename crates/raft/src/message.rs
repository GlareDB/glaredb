use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    Set { key: String, value: String },
    Begin,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Response {
    None,
    Begin(u64),
    Value(String),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ResponseValue {
    pub value: Option<String>,
}
