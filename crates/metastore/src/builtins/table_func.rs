//! Builtin table returning functions.
use crate::errors::Result;
use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema},
    datasource::TableProvider,
    scalar::ScalarValue,
};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum FunctionError {
    #[error("Invalid number of arguments.")]
    InvalidNumArgs,

    #[error("Unexpected argument for function. Got '{scalar}', need value of type '{expected}'")]
    UnexpectedArg {
        scalar: ScalarValue,
        expected: DataType,
    },
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
    funcs: HashMap<String, Box<dyn TableFunc>>,
}

impl BuiltinTableFuncs {
    pub fn find_function(
        &self,
        name: &str,
        args: &[ScalarValue],
    ) -> Result<&dyn TableFunc, FunctionError> {
        let func = self.funcs.get(name).unwrap();
        Ok(func.as_ref())
    }
}

#[async_trait]
pub trait TableFunc {
    fn parameters(&self) -> &[TableFuncParamaters];
    async fn create_table_provider(
        &self,
        args: &[ScalarValue],
    ) -> Result<Arc<dyn TableProvider>, FunctionError>;
}

pub struct ReadPostgres;

#[async_trait]
impl TableFunc for ReadPostgres {
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
                let conn_str = string_from_scalar(args[0])?;
                let schema = string_from_scalar(args[1])?;
                let table = string_from_scalar(args[2])?;
                unimplemented!()
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
