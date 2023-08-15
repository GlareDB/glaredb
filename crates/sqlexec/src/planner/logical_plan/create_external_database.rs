use protogen::FromOptionalField;

use super::*;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CreateExternalDatabase {
    pub database_name: String,
    pub if_not_exists: bool,
    pub options: DatabaseOptions,
    pub tunnel: Option<String>,
}

impl TryFrom<protogen::gen::metastore::service::CreateExternalDatabase> for CreateExternalDatabase {
    type Error = ProtoConvError;

    fn try_from(
        value: protogen::gen::metastore::service::CreateExternalDatabase,
    ) -> Result<Self, Self::Error> {
        let database_name = value.name;
        let if_not_exists = value.if_not_exists;
        let options = value.options.required("options")?;

        Ok(Self {
            database_name,
            if_not_exists,
            options,
            tunnel: value.tunnel,
        })
    }
}

impl UserDefinedLogicalNodeCore for CreateExternalDatabase {
    fn name(&self) -> &str {
        Self::EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&DfLogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &EMPTY_SCHEMA
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CreateExternalDatabase")
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl ExtensionType for CreateExternalDatabase {
    const EXTENSION_NAME: &'static str = "CreateExternalDatabase";
    fn try_decode_extension(extension: &LogicalPlanExtension) -> Result<Self> {
        match extension.node.as_any().downcast_ref::<Self>() {
            Some(s) => Ok(s.clone()),
            None => Err(internal!(
                "CreateExternalDatabase::try_decode_extension failed",
            )),
        }
    }

    fn try_encode(&self, buf: &mut Vec<u8>, _codec: &dyn LogicalExtensionCodec) -> Result<()> {
        use ::protogen::{
            gen::metastore::service as protogen,
            sqlexec::logical_plan::{LogicalPlanExtension, LogicalPlanExtensionType},
        };

        let proto = protogen::CreateExternalDatabase {
            name: self.database_name.clone(),
            options: Some(self.options.clone().into()),
            if_not_exists: self.if_not_exists,
            tunnel: self.tunnel.clone(),
        };
        let plan_type = LogicalPlanExtensionType::CreateExternalDatabase(proto);

        let lp_extension = LogicalPlanExtension {
            inner: Some(plan_type),
        };

        lp_extension
            .encode(buf)
            .map_err(|e| internal!("{}", e.to_string()))?;

        Ok(())
    }
}
