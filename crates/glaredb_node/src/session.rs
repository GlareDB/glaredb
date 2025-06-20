use glaredb_core::arrays::batch::Batch;
use glaredb_core::arrays::field::ColumnSchema;
use glaredb_core::arrays::format::pretty::components::PRETTY_COMPONENTS;
use glaredb_core::arrays::format::pretty::table::PrettyTable;
use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_error::DbError;
use glaredb_ext_csv::extension::CsvExtension;
use glaredb_ext_parquet::extension::ParquetExtension;
use glaredb_ext_spark::SparkExtension;
use glaredb_ext_tpch_gen::TpchGenExtension;
use glaredb_rt_native::runtime::{
    NativeSystemRuntime,
    ThreadedNativeExecutor,
    new_tokio_runtime_for_io,
};
use napi_derive::napi;

use crate::errors::Result;

const DEFAULT_TABLE_WIDTH: usize = 100;

pub fn connect() -> napi::Result<NodeSession> {
    let tokio_rt =
        new_tokio_runtime_for_io().map_err(|e| napi::Error::from_reason(e.to_string()))?;
    let runtime = NativeSystemRuntime::new(tokio_rt.handle().clone());
    let executor =
        ThreadedNativeExecutor::try_new().map_err(|e| napi::Error::from_reason(e.to_string()))?;

    let engine = SingleUserEngine::try_new(executor, runtime.clone())
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;

    engine
        .register_extension(SparkExtension)
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    engine
        .register_extension(TpchGenExtension)
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    engine
        .register_extension(CsvExtension)
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    engine
        .register_extension(ParquetExtension)
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;

    Ok(NodeSession {
        tokio_rt,
        engine: Some(engine),
    })
}

#[napi]
#[derive(Debug)]
pub struct NodeSession {
    #[allow(unused)]
    pub(crate) tokio_rt: tokio::runtime::Runtime,
    pub(crate) engine: Option<SingleUserEngine<ThreadedNativeExecutor, NativeSystemRuntime>>,
}

#[napi]
impl NodeSession {
    #[napi]
    pub async unsafe fn sql(&mut self, sql: String) -> napi::Result<NodeQueryResult> {
        unsafe { self.query(sql).await }
    }

    #[napi]
    pub async unsafe fn query(&mut self, sql: String) -> napi::Result<NodeQueryResult> {
        let session = self.try_get_engine()?.session().clone();

        let mut q_res = session
            .query(&sql)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let batches = q_res
            .output
            .collect()
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;

        let result = NodeQueryResult {
            schema: q_res.output_schema,
            batches,
        };

        Ok(result)
    }

    #[napi]
    pub fn close(&mut self) -> napi::Result<()> {
        match self.engine.take() {
            Some(_) => Ok(()),
            None => Err(napi::Error::from_reason(
                "Tried to close an already closed session".to_string(),
            )),
        }
    }
}

impl NodeSession {
    fn try_get_engine(
        &self,
    ) -> Result<&SingleUserEngine<ThreadedNativeExecutor, NativeSystemRuntime>> {
        let engine = self.engine.as_ref().ok_or_else(|| {
            DbError::new("Attempted to reuse session after it's already been closed")
        })?;
        Ok(engine)
    }
}

#[napi]
#[derive(Debug)]
pub struct NodeQueryResult {
    pub(crate) schema: ColumnSchema,
    pub(crate) batches: Vec<Batch>,
}

#[napi]
impl NodeQueryResult {
    #[napi]
    pub fn to_string(&self) -> napi::Result<String> {
        let pretty = PrettyTable::try_new(
            &self.schema,
            &self.batches,
            DEFAULT_TABLE_WIDTH,
            None,
            PRETTY_COMPONENTS,
        )
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(format!("{pretty}"))
    }

    #[napi]
    pub fn show(&self) -> napi::Result<()> {
        let pretty = PrettyTable::try_new(
            &self.schema,
            &self.batches,
            DEFAULT_TABLE_WIDTH,
            None,
            PRETTY_COMPONENTS,
        )
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        println!("{pretty}");
        Ok(())
    }
}
