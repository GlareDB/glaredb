use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::insert::DataSink;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::{DisplayFormatType, SendableRecordBatchStream};
use futures::StreamExt;
use lance::io::writer::FileWriterOptions;
use lance::io::FileWriter;
use object_store::{path::Path as ObjectPath, ObjectStore};

#[derive(Debug, Clone)]
pub struct LanceSinkOpts {
    pub disable_all_column_stats: Option<bool>,
    pub collect_all_column_stats: Option<bool>,
    pub column_stats: Option<Vec<String>>,
}

/// Writes parquet files to object storage.
#[derive(Debug, Clone)]
pub struct LanceSink {
    store: Arc<dyn ObjectStore>,
    loc: ObjectPath,
    opts: LanceSinkOpts,
}
impl Default for LanceSinkOpts {
    fn default() -> Self {
        LanceSinkOpts {
            collect_all_column_stats: Some(true),
            column_stats: None,
            disable_all_column_stats: None,
        }
    }
}

impl LanceSinkOpts {
    fn no_column_stats_specified(self) -> bool {
        return opts.column_stats.is_none()
            && opts.disable_all_column_stats.is_none()
            && opts.collect_all_column_stats.is_none();
    }
    fn conflicting_user_specified_stats(self) -> bool {
        return opts.column_stats.is_some()
            && (opts.disable_all_column_stats.is_some()
                || opts.collect_all_column_stats.is_some());
    }
    fn conflicting_stats_options(self) -> bool {
        return opts.disable_all_column_stats.is_some() && opts.collect_all_column_stats.is_some();
    }
    fn is_invalid(self) -> bool {
        return self.no_column_stats_specified()
            || self.conflicting_stats_options()
            || self.conflicting_user_specified_stats();
    }
}

impl fmt::Display for LanceSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LanceSink({}:{})", self.store, self.loc)
    }
}

impl DisplayAs for LanceSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "{self}"),
            DisplayFormatType::Verbose => write!(f, "{self}"),
        }
    }
}

impl LanceSink {
    pub fn from_obj_store(store: Arc<dyn ObjectStore>, loc: impl Into<ObjectPath>) -> Self {
        LanceSink {
            store,
            loc: loc.into(),
            opts: LanceSinkOpts::default(),
        }
    }

    pub fn with_options(self, opts: LanceSinkOpts) -> Self {
        if self.is_invalid() {
            return self;
        }

        let mut out = self;
        out.opts = Some(opts);
        self
    }

    async fn stream_into_inner(&self, mut stream: SendableRecordBatchStream) -> DfResult<usize> {
        let mut opts = FileWriterOptions::default();
        if self
            .opts
            .collect_all_column_stats
            .is_some_and(|val| val == true)
        {
            opts.collect_stats_for_fields = stream
                .schema()
                .fields
                .iter()
                .enumerate()
                .map(|v| v.0 as i32)
                .collect();
        } else if self.opts.column_stats.is_some() {
            let set = HashSet::from_iter(self.opts.column_stats.unwrap().iter());

            opts.collect_stats_for_fields = stream
                .schema()
                .fields
                .iter()
                .filter(|f| set.contains(f))
                .enumerate()
                .map(|v| v.0 as i32)
                .collect();
        }

        let mut writer = FileWriter::with_object_writer(
            lance::io::ObjectWriter::new(self.store.as_ref(), &self.loc).await?,
            lance::datatypes::Schema::try_from(stream.schema().as_ref())?,
            &opts,
        )?;

        while let Some(batch) = stream.next().await {
            let batch = batch?;
            writer.write(&[batch]).await?;
        }

        writer.finish().await.map_err(|e| e.into())
    }
}

#[async_trait]
impl DataSink for LanceSink {
    async fn write_all(
        &self,
        data: Vec<SendableRecordBatchStream>,
        _context: &Arc<TaskContext>,
    ) -> DfResult<u64> {
        let mut count = 0;
        for stream in data {
            count += self.stream_into_inner(stream).await.map(|x| x as u64)?;
        }
        Ok(count)
    }
}
