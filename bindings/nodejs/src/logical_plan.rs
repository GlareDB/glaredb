use sqlexec::LogicalPlan;

use crate::{
    connection::JsTrackedSession, error::JsGlareDbError, execution_result::JsExecutionResult,
};

#[napi]
#[derive(Clone, Debug)]
pub struct JsLogicalPlan {
    pub(crate) lp: LogicalPlan,
    pub(crate) session: JsTrackedSession,
}

impl JsLogicalPlan {
    pub(super) fn new(lp: LogicalPlan, session: JsTrackedSession) -> Self {
        Self { lp, session }
    }

    async fn execute_inner(&self) -> napi::Result<JsExecutionResult> {
        let mut sess = self.session.lock().await;
        let (_, stream) = sess
            .execute_inner(self.lp.clone())
            .await
            .map_err(JsGlareDbError::from)?;

        Ok(JsExecutionResult(stream))
    }
}

#[napi]
impl JsLogicalPlan {
    #[napi(catch_unwind)]
    pub fn to_string(&self) -> napi::Result<String> {
        Ok(format!("{:?}", self.lp))
    }

    #[napi(catch_unwind)]
    pub async fn show(&self) -> napi::Result<()> {
        self.execute_inner().await?.show().await?;
        Ok(())
    }

    #[napi(catch_unwind)]
    pub async fn execute(&self) -> napi::Result<()> {
        self.execute_inner().await?.execute().await?;
        Ok(())
    }

    #[napi(catch_unwind)]
    pub async fn record_batches(&self) -> napi::Result<Vec<crate::record_batch::JsRecordBatch>> {
        self.execute_inner().await?.record_batches().await
    }

    #[napi(catch_unwind)]
    pub async fn to_ipc(&self) -> napi::Result<napi::bindgen_prelude::Buffer> {
        let inner = self.execute_inner().await?.to_arrow_inner().await?;
        Ok(inner.into())
    }

    #[napi(ts_return_type = "pl.DataFrame")]
    /// Convert to a Polars DataFrame.
    /// "nodejs-polars" must be installed as a peer dependency.
    /// See https://www.npmjs.com/package/nodejs-polars
    pub async fn to_polars(&self) -> napi::Result<()> {
        // TODO: implement this in rust if possible?
        // Currently, this is monkeypatched in js-glaredb/glaredb.js
        unimplemented!("to_polars")
    }

    #[napi(ts_return_type = "arrow.Table<any>")]
    /// Convert to an "apache-arrow" Table.
    /// "apache-arrow" must be installed as a peer dependency.
    /// See https://www.npmjs.com/package/apache-arrow
    pub async fn to_arrow(&self) -> napi::Result<()> {
        // TODO: implement this in rust if possible?
        // Currently, this is monkeypatched in js-glaredb/glaredb.js
        unimplemented!("to_arrow")
    }
}
