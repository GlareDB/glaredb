//! Builtin table returning functions.
use crate::errors::Result;
use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema},
    datasource::TableProvider,
    scalar::ScalarValue,
};
use datasources::postgres::{PostgresAccessor, PostgresTableAccess};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Arc;

/// Builtin table returning functions available for all sessions.
pub static BUILTIN_TABLE_FUNCS: Lazy<BuiltinTableFuncs> = Lazy::new(|| BuiltinTableFuncs::new());

#[derive(Debug, thiserror::Error)]
pub enum FunctionError {
    #[error("Invalid number of arguments.")]
    InvalidNumArgs,

    #[error("Unexpected argument for function. Got '{scalar}', need value of type '{expected}'")]
    UnexpectedArg {
        scalar: ScalarValue,
        expected: DataType,
    },

    #[error(transparent)]
    Access(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, Clone)]
pub struct TableFuncParameter {
    pub name: &'static str,
    pub typ: DataType,
}

#[derive(Debug, Clone)]
pub struct TableFuncParamaters {
    pub params: &'static [TableFuncParameter],
}

pub struct BuiltinTableFuncs {
    funcs: HashMap<String, Arc<dyn TableFunc>>,
}

impl BuiltinTableFuncs {
    pub fn new() -> BuiltinTableFuncs {
        let funcs: Vec<Arc<dyn TableFunc>> = vec![Arc::new(ReadPostgres)];
        let funcs: HashMap<String, Arc<dyn TableFunc>> = funcs
            .into_iter()
            .map(|f| (f.name().to_string(), f))
            .collect();

        BuiltinTableFuncs { funcs }
    }

    pub fn find_function(&self, name: &str) -> Option<Arc<dyn TableFunc>> {
        self.funcs.get(name).cloned()
    }

    pub fn iter_funcs(&self) -> impl Iterator<Item = &Arc<dyn TableFunc>> {
        self.funcs.iter().map(|(_, f)| f)
    }
}

#[async_trait]
pub trait TableFunc: Sync + Send {
    fn name(&self) -> &str;

    fn parameters(&self) -> &[TableFuncParamaters];

    async fn create_table_provider(
        &self,
        args: &[ScalarValue],
    ) -> Result<Arc<dyn TableProvider>, FunctionError>;
}

#[derive(Debug, Clone, Copy)]
pub struct ReadPostgres;

#[async_trait]
impl TableFunc for ReadPostgres {
    fn name(&self) -> &str {
        "read_postgres"
    }

    fn parameters(&self) -> &[TableFuncParamaters] {
        const PARAMS: &'static [TableFuncParamaters] = &[TableFuncParamaters {
            params: &[
                TableFuncParameter {
                    name: "connection_str",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "schema",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "table",
                    typ: DataType::Utf8,
                },
            ],
        }];

        PARAMS
    }

    async fn create_table_provider(
        &self,
        args: &[ScalarValue],
    ) -> Result<Arc<dyn TableProvider>, FunctionError> {
        match args.len() {
            3 => {
                let conn_str = string_from_scalar(&args[0])?;
                let schema = string_from_scalar(&args[1])?;
                let table = string_from_scalar(&args[2])?;

                let access = PostgresAccessor::connect(conn_str, None)
                    .await
                    .map_err(|e| FunctionError::Access(Box::new(e)))?;
                let prov = access
                    .into_table_provider(
                        PostgresTableAccess {
                            schema: schema.clone(),
                            name: table.clone(),
                        },
                        true,
                    )
                    .await
                    .map_err(|e| FunctionError::Access(Box::new(e)))?;

                Ok(Arc::new(prov))
            }
            _ => Err(FunctionError::InvalidNumArgs),
        }
    }
}

fn string_from_scalar(val: &ScalarValue) -> Result<&String, FunctionError> {
    match val {
        ScalarValue::Utf8(Some(s)) => Ok(s),
        other => Err(FunctionError::UnexpectedArg {
            scalar: other.clone(),
            expected: DataType::Utf8,
        }),
    }
}
