//! Serde compatible postgres protocol messages.
//!
//! These messages exist seperately from the messages defined in pgsrv to avoid
//! having to deal possible incompatibilities between types and being able to
//! easily serialize/deserialize in a human readonable format.
use anyhow::{anyhow, Error};
use postgres_protocol::message::{backend::Message, frontend};
use serde::{Deserialize, Serialize};
use std::fmt;

/// A human-readable serialized backend message.
pub struct SerializedMessage {
    pub typ: String,
    pub json: String,
}

impl fmt::Display for SerializedMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.typ, self.json)
    }
}

impl TryFrom<(char, Message)> for SerializedMessage {
    type Error = Error;

    fn try_from((id, msg): (char, Message)) -> Result<Self, Self::Error> {
        let (typ, json) = match msg {
            Message::ReadyForQuery(msg) => (
                "ReadyForQuery",
                serde_json::to_string(&ReadyForQuery {
                    status: String::from(msg.status() as char),
                })?,
            ),
            _ => return Err(anyhow!("unhandle message, type identifier: {}", id)),
        };
        Ok(SerializedMessage {
            typ: typ.to_string(),
            json: json.to_string(),
        })
    }
}

// Frontend messages.

#[derive(Deserialize)]
pub struct Query {
    pub query: String,
}

#[derive(Deserialize)]
pub struct Parse {
    pub name: Option<String>,
    pub query: String,
}

#[derive(Deserialize)]
pub struct Bind {
    pub portal: Option<String>,
    pub statement: Option<String>,
    pub values: Option<Vec<String>>,
    pub result_formats: Option<Vec<i16>>,
}

#[derive(Deserialize)]
pub struct Execute {
    pub portal: Option<String>,
    pub max_rows: Option<i32>,
}

#[derive(Deserialize)]
pub struct Describe {
    pub variant: Option<String>,
    pub name: Option<String>,
}

// Backend messages.

#[derive(Serialize)]
pub struct ReadyForQuery {
    pub status: String,
}

#[derive(Serialize)]
pub struct RowDescription {
    pub fields: Vec<Field>,
}

#[derive(Serialize)]
pub struct Field {
    pub name: String,
}

#[derive(Serialize)]
pub struct DataRow {
    pub fields: Vec<String>,
}

#[derive(Serialize)]
pub struct ParameterDescription {
    pub parameters: Vec<u32>,
}

#[derive(Serialize)]
pub struct CommandComplete {
    pub tag: String,
}
