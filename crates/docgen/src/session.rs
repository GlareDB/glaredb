use rayexec_error::Result;
use rayexec_execution::runtime::{Runtime, TokioHandlerProvider};
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use rayexec_shell::result_table::MaterializedResultTable;
use rayexec_shell::session::{SingleUserEngine, SingleUserSession};

#[derive(Debug)]
pub struct DocsSession {
    pub engine: SingleUserEngine<ThreadedNativeExecutor, NativeRuntime>,
}

impl DocsSession {
    pub fn query(&self, sql: &str) -> Result<MaterializedResultTable> {
        let handle = self.engine.runtime.tokio_handle().handle()?;
        let fut = self.engine.session().query(sql);

        let result: Result<MaterializedResultTable> = handle.block_on(async move {
            let table = fut.await?;
            let materialized = table.collect().await?;

            Ok(materialized)
        });

        result
    }
}
