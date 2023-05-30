//! Adapted from DataFusion's experimental push-based scheduler. It's since been
//! removed in this [PR].
//!
//! [PR]: https://github.com/apache/arrow-datafusion/pull/6169
//!
//! A [`Scheduler`] maintains a pool of dedicated worker threads on which query
//! execution can be scheduled. This is based on the idea of [Morsel-Driven
//! Parallelism] and is designed to decouple the execution parallelism from the
//! parallelism expressed in the physical plan as partitions.
//!
//! # Implementation
//!
//! When provided with an [`ExecutionPlan`] the [`Scheduler`] first breaks it up
//! into smaller chunks called pipelines. Each pipeline may consist of one or
//! more nodes from the [`ExecutionPlan`] tree.
//!
//! The scheduler then maintains a list of pending [`Task`], that identify a
//! partition within a particular pipeline that may be able to make progress on
//! some "morsel" of data. These [`Task`] are then scheduled on the worker pool,
//! with a preference for scheduling work on a given "morsel" on the same thread
//! that produced it.
//!
//! # Rayon
//!
//! Under-the-hood these [`Task`] are scheduled by [rayon], which is a
//! lightweight, work-stealing scheduler optimised for CPU-bound workloads.
//! Pipelines may exploit this fact, and use [rayon]'s structured concurrency
//! primitives to express additional parallelism that may be exploited if there
//! are idle threads available at runtime
//!
//! # Shutdown
//!
//! Queries scheduled on a [`Scheduler`] will run to completion even if the
//! [`Scheduler`] is dropped
//!
//! [Morsel-Driven Parallelism]: https://db.in.tum.de/~leis/papers/morsels.pdf
//! [rayon]: https://docs.rs/rayon/latest/rayon/

use datafusion::{execution::context::TaskContext, physical_plan::ExecutionPlan};
use pipeline::{ErrorSink, Sink};
use plan::{PipelinePlan, PipelinePlanner};
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::sync::Arc;
use task::{spawn_plan, Task};
use tracing::{debug, error};

pub mod errors;
pub mod runtime;
pub mod stream;

mod pipeline;
mod plan;
mod task;

use errors::Result;

/// A `Scheduler` that can be used to schedule `ExecutionPlan` on a dedicated
/// thread pool.
pub struct Scheduler {
    pool: Arc<ThreadPool>,
}

impl Scheduler {
    pub fn new(pool: Arc<ThreadPool>) -> Scheduler {
        Scheduler { pool }
    }

    /// Schedule the provided [`ExecutionPlan`] on this [`Scheduler`].
    ///
    /// Returns a [`ExecutionResults`] that can be used to receive results as they are produced,
    /// as a [`futures::Stream`] of [`RecordBatch`]
    pub fn schedule(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
        sink: Arc<dyn Sink>,
        error_sink: Arc<dyn ErrorSink>,
    ) -> Result<()> {
        let plan = PipelinePlanner::new(plan, context).build(sink, error_sink)?;
        self.schedule_plan(plan);
        Ok(())
    }

    /// Schedule the provided [`PipelinePlan`] on this [`Scheduler`].
    pub(crate) fn schedule_plan(&self, plan: PipelinePlan) {
        spawn_plan(plan, self.spawner())
    }

    fn spawner(&self) -> Spawner {
        Spawner {
            pool: self.pool.clone(),
        }
    }
}

/// Formats a panic message for a worker
fn format_worker_panic(panic: Box<dyn std::any::Any + Send>) -> String {
    let maybe_idx = rayon::current_thread_index();
    let worker: &dyn std::fmt::Display = match &maybe_idx {
        Some(idx) => idx,
        None => &"UNKNOWN",
    };

    let message = if let Some(msg) = panic.downcast_ref::<&str>() {
        *msg
    } else if let Some(msg) = panic.downcast_ref::<String>() {
        msg.as_str()
    } else {
        "UNKNOWN"
    };

    format!("worker {} panicked with: {}", worker, message)
}

#[derive(Debug, Clone)]
pub struct Spawner {
    pool: Arc<ThreadPool>,
}

impl Spawner {
    pub fn spawn(&self, task: Task) {
        debug!(?task, "spawning task to any worker");
        self.pool.spawn(move || task.do_work());
    }

    pub fn spawn_fifo(&self, task: Task) {
        debug!(?task, "spawning task to any worker (fifo)");
        self.pool.spawn_fifo(move || task.do_work());
    }
}

#[cfg(test)]
mod tests {
    use crate::stream::create_coalescing_adapter;
    use datafusion::arrow::array::{ArrayRef, PrimitiveArray};
    use datafusion::arrow::datatypes::{ArrowPrimitiveType, Float64Type, Int32Type};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::datasource::{MemTable, TableProvider};
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use futures::{StreamExt, TryStreamExt};
    use rand::distributions::uniform::SampleUniform;
    use rand::{thread_rng, Rng};
    use std::ops::Range;

    use super::*;

    fn generate_primitive<T, R>(
        rng: &mut R,
        len: usize,
        valid_percent: f64,
        range: Range<T::Native>,
    ) -> ArrayRef
    where
        T: ArrowPrimitiveType,
        T::Native: SampleUniform,
        R: Rng,
    {
        Arc::new(PrimitiveArray::<T>::from_iter((0..len).map(|_| {
            rng.gen_bool(valid_percent)
                .then(|| rng.gen_range(range.clone()))
        })))
    }

    fn generate_batch<R: Rng>(rng: &mut R, row_count: usize, id_offset: i32) -> RecordBatch {
        let id_range = id_offset..(row_count as i32 + id_offset);
        let a = generate_primitive::<Int32Type, _>(rng, row_count, 0.5, 0..1000);
        let b = generate_primitive::<Float64Type, _>(rng, row_count, 0.5, 0. ..1000.);
        let id = PrimitiveArray::<Int32Type>::from_iter_values(id_range);

        RecordBatch::try_from_iter_with_nullable([
            ("a", a, true),
            ("b", b, true),
            ("id", Arc::new(id), false),
        ])
        .unwrap()
    }

    const BATCHES_PER_PARTITION: usize = 20;
    const ROWS_PER_BATCH: usize = 100;
    const NUM_PARTITIONS: usize = 2;

    fn make_batches() -> Vec<Vec<RecordBatch>> {
        let mut rng = thread_rng();

        let mut id_offset = 0;

        (0..NUM_PARTITIONS)
            .map(|_| {
                (0..BATCHES_PER_PARTITION)
                    .map(|_| {
                        let batch = generate_batch(&mut rng, ROWS_PER_BATCH, id_offset);
                        id_offset += ROWS_PER_BATCH as i32;
                        batch
                    })
                    .collect()
            })
            .collect()
    }

    fn make_provider() -> Arc<dyn TableProvider> {
        let batches = make_batches();
        let schema = batches.first().unwrap().first().unwrap().schema();
        Arc::new(MemTable::try_new(schema, make_batches()).unwrap())
    }

    #[tokio::test]
    async fn test_simple() {
        let scheduler = Scheduler::new(Arc::new(ThreadPoolBuilder::new().build().unwrap()));

        let config = SessionConfig::new().with_target_partitions(4);
        let context = SessionContext::with_config(config);

        context.register_table("table1", make_provider()).unwrap();
        context.register_table("table2", make_provider()).unwrap();

        let queries = [
            "select * from table1 order by id",
            "select * from table1 where table1.a > 100 order by id",
            "select distinct a from table1 where table1.b > 100 order by a",
            "select * from table1 join table2 on table1.id = table2.id order by table1.id",
            "select id from table1 union all select id from table2 order by id",
            "select id from table1 union all select id from table2 where a > 100 order by id",
            "select id, b from (select id, b from table1 union all select id, b from table2 where a > 100 order by id) as t where b > 10 order by id, b",
            "select id, MIN(b), MAX(b), AVG(b) from table1 group by id order by id",
            "select count(*) from table1 where table1.a > 4",
        ];

        for sql in queries {
            let task = context.task_ctx();

            let query = context.sql(sql).await.unwrap();

            let plan = query.clone().create_physical_plan().await.unwrap();

            println!("Plan: {}", displayable(plan.as_ref()).indent());

            let (sink, stream) =
                create_coalescing_adapter(plan.output_partitioning(), plan.schema());
            let sink = Arc::new(sink);

            scheduler.schedule(plan, task, sink.clone(), sink).unwrap();
            let scheduled: Vec<_> = stream.try_collect().await.unwrap();
            let expected = query.collect().await.unwrap();

            let total_expected = expected.iter().map(|x| x.num_rows()).sum::<usize>();
            let total_scheduled = scheduled.iter().map(|x| x.num_rows()).sum::<usize>();
            assert_eq!(total_expected, total_scheduled);

            println!("Query \"{}\" produced {} rows", sql, total_expected);

            let expected = pretty_format_batches(&expected).unwrap().to_string();
            let scheduled = pretty_format_batches(&scheduled).unwrap().to_string();

            assert_eq!(
                expected, scheduled,
                "\n\nexpected:\n\n{}\nactual:\n\n{}\n\n",
                expected, scheduled
            );
        }
    }
}
