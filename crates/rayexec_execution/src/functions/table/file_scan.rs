use std::fmt::Debug;
use std::future::Future;

use crate::execution::operators::source::operation::SourceOperation;
use crate::io::file::FileOpener;

pub trait FileScan: Debug {
    type Operation: SourceOperation;

    fn plan_source_operation<F>(
        &self,
        fs: &F,
        conf: &F::AccessConfig,
    ) -> impl Future<Output = Self::Operation> + Send
    where
        F: FileOpener;
}
