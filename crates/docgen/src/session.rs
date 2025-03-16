use glaredb_error::Result;
use rayexec_execution::arrays::batch::Batch;
use rayexec_execution::arrays::field::ColumnSchema;
use rayexec_execution::engine::single_user::SingleUserEngine;
use rayexec_execution::runtime::{Runtime, TokioHandlerProvider};
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};

#[derive(Debug)]
pub struct DocsSession {
    pub engine: SingleUserEngine<ThreadedNativeExecutor, NativeRuntime>,
}

impl DocsSession {
    pub fn query(&self, sql: &str) -> Result<DocsQueryResult> {
        let handle = self.engine.runtime.tokio_handle().handle()?;
        let fut = self.engine.session().query(sql);

        let result: Result<DocsQueryResult> = handle.block_on(async move {
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
