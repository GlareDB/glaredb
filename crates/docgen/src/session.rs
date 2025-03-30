use glaredb_core::arrays::batch::Batch;
use glaredb_core::arrays::field::ColumnSchema;
use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_error::Result;
use glaredb_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use tokio::runtime::Runtime as TokioRuntime;

#[derive(Debug)]
pub struct DocsSession {
    pub tokio_rt: TokioRuntime,
    pub engine: SingleUserEngine<ThreadedNativeExecutor, NativeRuntime>,
}

impl DocsSession {
    pub fn query(&self, sql: &str) -> Result<DocsQueryResult> {
        let fut = self.engine.session().query(sql);

        let result: Result<DocsQueryResult> = self.tokio_rt.block_on(async move {
            let mut q_res = fut.await?;
            let batches = q_res.output.collect().await?;

            Ok(DocsQueryResult {
                schema: q_res.output_schema,
                batches,
            })
        });

        result
    }
}

#[derive(Debug)]
pub struct DocsQueryResult {
    pub schema: ColumnSchema,
    pub batches: Vec<Batch>,
}
