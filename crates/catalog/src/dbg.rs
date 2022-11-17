use catalog_types::context::SessionContext;
use datafusion::arrow::compute::kernels::concat::concat_batches;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use futures::executor;
use futures::TryStreamExt;

/// Print out the contents of a table.
pub fn dbg_table<T: TableProvider + ?Sized, R: AsRef<T>>(ctx: &SessionContext, table: R) {
    executor::block_on(async {
        let plan = table
            .as_ref()
            .scan(ctx.get_df_state(), &None, &[], None)
            .await
            .unwrap();
        let stream = plan.execute(0, ctx.task_context()).unwrap();
        let schema = stream.schema();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let batch = concat_batches(&schema, &batches).unwrap();

        println!("{:?}", batch);
    })
}
