use uuid::Uuid;

use super::common::SessionStorageConfig;
use crate::errors::ProtoConvError;
use crate::gen::rpcsrv::simple;
use crate::FromOptionalField;

#[derive(Debug, Clone)]
pub struct ExecuteQueryRequest {
    pub config: SessionStorageConfig,
    pub database_id: Uuid,
    pub query_text: String,
}

impl TryFrom<simple::ExecuteQueryRequest> for ExecuteQueryRequest {
    type Error = ProtoConvError;

    fn try_from(value: simple::ExecuteQueryRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            config: value.config.required("config")?,
            database_id: Uuid::from_slice(&value.database_id)?,
            query_text: value.query_text,
        })
    }
}

impl From<ExecuteQueryRequest> for simple::ExecuteQueryRequest {
    fn from(value: ExecuteQueryRequest) -> Self {
        Self {
            config: Some(value.config.into()),
            database_id: value.database_id.into_bytes().into(),
            query_text: value.query_text,
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueryResultSuccess {}

impl From<simple::QueryResultSuccess> for QueryResultSuccess {
    fn from(_value: simple::QueryResultSuccess) -> Self {
        Self {}
    }
}

impl From<QueryResultSuccess> for simple::QueryResultSuccess {
    fn from(_value: QueryResultSuccess) -> Self {
        Self {}
    }
}

#[derive(Debug, Clone)]
pub struct QueryResultError {
    pub msg: String,
}

impl From<simple::QueryResultError> for QueryResultError {
    fn from(value: simple::QueryResultError) -> Self {
        Self { msg: value.msg }
    }
}

impl From<QueryResultError> for simple::QueryResultError {
    fn from(value: QueryResultError) -> Self {
        Self { msg: value.msg }
    }
}

#[derive(Debug, Clone)]
pub enum ExecuteQueryResponse {
    SuccessResult(QueryResultSuccess),
    ErrorResult(QueryResultError),
}

impl TryFrom<simple::ExecuteQueryResponse> for ExecuteQueryResponse {
    type Error = ProtoConvError;

    fn try_from(value: simple::ExecuteQueryResponse) -> Result<Self, Self::Error> {
        let result: simple::execute_query_response::Result = value.result.required("result")?;
        Ok(match result {
            simple::execute_query_response::Result::Success(success) => {
                Self::SuccessResult(success.into())
            }
            simple::execute_query_response::Result::Error(error) => Self::ErrorResult(error.into()),
        })
    }
}

impl From<ExecuteQueryResponse> for simple::ExecuteQueryResponse {
    fn from(value: ExecuteQueryResponse) -> Self {
        match value {
            ExecuteQueryResponse::SuccessResult(success) => simple::ExecuteQueryResponse {
                result: Some(simple::execute_query_response::Result::Success(
                    success.into(),
                )),
            },
            ExecuteQueryResponse::ErrorResult(error) => simple::ExecuteQueryResponse {
                result: Some(simple::execute_query_response::Result::Error(error.into())),
            },
        }
    }
}
