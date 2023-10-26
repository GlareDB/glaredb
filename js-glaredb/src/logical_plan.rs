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
  fn __repr__(&self) -> napi::Result<String> {
    Ok(format!("{:#?}", self.lp))
  }

  #[napi]
  pub async fn show(&self) -> napi::Result<()> {
    self.execute_inner().await?.show().await?;
    Ok(())
  }

  #[napi]
  pub async fn execute(&self) -> napi::Result<()> {
    unsafe { self.execute_inner().await?.execute().await? };
    Ok(())
  }

  #[napi]
  pub async fn record_batches(&self) -> napi::Result<Vec<crate::record_batch::JsRecordBatch>> {
    Ok(unsafe { self.execute_inner().await?.record_batches().await? })
  }

  #[napi]
  pub async fn to_arrow(&self) -> napi::Result<napi::bindgen_prelude::Buffer> {
    let inner = unsafe { self.execute_inner().await?.to_arrow_inner().await? };
    Ok(inner.into())
  }
}
