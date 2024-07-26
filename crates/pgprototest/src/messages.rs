//! Serde compatible postgres protocol messages.
//!
//! These messages exist seperately from the messages defined in pgsrv to avoid
//! having to deal possible incompatibilities between types and being able to
//! easily serialize/deserialize in a human readonable format. It also allows
//! to omit fields that we don't currently care about.
use std::fmt;

use anyhow::{anyhow, Error};
use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend::Message;
use serde::{Deserialize, Serialize};

/// A human-readable serialized backend message.
pub struct SerializedMessage {
    /// The message type, e.g. "ReadyForQuery".
    pub typ: String,
    /// The serialized message.
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
            Message::RowDescription(msg) => (
                "RowDescription",
                serde_json::to_string(&RowDescription {
                    fields: msg
                        .fields()
                        .map(|field| {
                            Ok(Field {
                                name: field.name().to_string(),
                            })
                        })
                        .collect()?,
                })?,
            ),
            Message::DataRow(msg) => (
                "DataRow",
                serde_json::to_string(&DataRow {
                    fields: msg
                        .ranges()
                        .map(|range| match range {
                            Some(range) => Ok(String::from_utf8(
                                msg.buffer()[range.start..range.end].to_vec(),
                            )
                            .unwrap()), // TODO: Print raw bytes instead.
                            None => Ok(String::from("NULL")),
                        })
                        .collect()?,
                })?,
            ),
            Message::CommandComplete(msg) => (
                "CommandComplete",
                serde_json::to_string(&CommandComplete {
                    tag: msg.tag()?.to_string(),
                })?,
            ),
            Message::ParseComplete => ("ParseComplete", String::new()),
            Message::BindComplete => ("BindComplete", String::new()),
            Message::CloseComplete => ("CloseComplete", String::new()),
            Message::NoData => ("NoData", String::new()),
            Message::EmptyQueryResponse => ("EmptyQueryResponse", String::new()),
            Message::ErrorResponse(msg) => (
                "ErrorResponse",
                serde_json::to_string(&ErrorResponse {
                    fields: msg
                        .fields()
                        .map(|field| Ok(String::from_utf8(field.value_bytes().to_owned()).unwrap()))
                        .collect()?,
                })?,
            ),
            Message::NoticeResponse(msg) => (
                "NoticeResponse",
                serde_json::to_string(&ErrorResponse {
                    fields: msg
                        .fields()
                        .map(|field| Ok(String::from_utf8(field.value_bytes().to_owned()).unwrap()))
                        .collect()?,
                })?,
            ),
            _ => return Err(anyhow!("unhandle message, type identifier: {}", id)),
        };
        Ok(SerializedMessage {
            typ: typ.to_string(),
            json,
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
pub struct CloseStatement {
    pub name: Option<String>,
}

#[derive(Deserialize)]
pub struct ClosePortal {
    pub name: Option<String>,
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

#[derive(Serialize)]
pub struct ErrorResponse {
    pub fields: Vec<String>,
}

#[derive(Serialize)]
pub struct NoticeResponse {
    pub fields: Vec<String>,
}
