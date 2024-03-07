use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Signature, TypeSignature, Volatility};
use datafusion_ext::errors::Result;
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider};
use datasources::debug::{validate_tunnel_connections, DebugTableProvider, DebugTableType};
pub use datasources::Datasource;
use parser::errors::ParserError;
use protogen::metastore::types::catalog::{FunctionType, RuntimePreference, TableEntry};
use protogen::metastore::types::options::{
    CredentialsOptions,
    TableOptions,
    TableOptionsDebug,
    TunnelOptions,
};

use super::TableFunc;
use crate::functions::ConstBuiltinFunction;

#[derive(Debug, Clone, Copy)]
pub struct Dummy;

impl ConstBuiltinFunction for Dummy {
    const NAME: &'static str = "debug";
    const DESCRIPTION: &'static str = "A dummy table function for debugging";
    const EXAMPLE: &'static str = "SELECT * FROM debug('never_ending') limit 1";
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;

    fn signature(&self) -> Option<Signature> {
        Some(Signature::one_of(
            vec![
                TypeSignature::Any(0),
                TypeSignature::Exact(vec![DataType::Utf8]),
            ],
            Volatility::Stable,
        ))
    }
}

#[async_trait]
impl TableFunc for Dummy {
    fn detect_runtime(
        &self,
        _args: &[FuncParamValue],
        _parent: RuntimePreference,
    ) -> Result<RuntimePreference> {
        Ok(RuntimePreference::Local)
    }

    async fn create_provider(
        &self,
        _: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        _opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        let typ = match args.len() {
            0 => DebugTableType::NeverEnding,
            1 => {
                let s: String = args[0].clone().try_into()?;
                s.as_str().parse().unwrap()
            }
            _ => todo!(),
        };
        Ok(Arc::new(DebugTableProvider { typ, tunnel: false }))
    }
}

#[derive(Debug, Clone)]
pub struct TableOptionsDebug2 {
    pub table_type: String,
}

impl From<TableOptionsDebug2> for TableOptions {
    fn from(value: TableOptionsDebug2) -> Self {
        let mut options = BTreeMap::new();
        options.insert("table_type".to_string(), value.table_type.into());

        TableOptions {
            name: "debug".to_string(),
            options,
        }
    }
}

pub struct DebugDatasource {}

#[async_trait]
impl Datasource for DebugDatasource {
    const NAME: &'static str = "debug";


    fn table_options_from_stmt(
        opts: &mut parser::options::StatementOptions,
        creds: Option<protogen::metastore::types::options::CredentialsOptions>,
        tunnel_opts: Option<TunnelOptions>,
    ) -> Result<impl Into<TableOptions>, ParserError>
    where
        Self: Sized,
    {
        validate_tunnel_connections(tunnel_opts.as_ref()).unwrap();

        let typ: Option<DebugTableType> = match creds {
            Some(CredentialsOptions::Debug(c)) => c.table_type.parse().ok(),
            Some(other) => unreachable!("invalid credentials {other} for debug datasource"),
            None => None,
        };
        let typ: DebugTableType = opts.remove_required_or("table_type", typ)?;

        Ok(TableOptionsDebug2 {
            table_type: typ.to_string(),
        })
    }

    async fn dispatch_table_entry_with_tunnel<BuiltinError>(
        entry: &TableEntry,
        tunnel_opts: Option<&TunnelOptions>,
    ) -> Result<Arc<dyn TableProvider>, BuiltinError> {
        let options = TableOptionsDebug::try_from(&entry.options).unwrap();


        let typ: DebugTableType = options.table_type.parse().unwrap();
        let tunnel = validate_tunnel_connections(tunnel_opts)
            .expect("datasources should be validated with tunnels before dispatch");
        Ok(Arc::new(DebugTableProvider { typ, tunnel }))
    }
}
