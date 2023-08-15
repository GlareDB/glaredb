use super::*;
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CreateCredentials {
    pub name: String,
    pub options: CredentialsOptions,
    pub comment: String,
}

impl TryFrom<protogen::gen::metastore::service::CreateCredentials> for CreateCredentials {
    type Error = ProtoConvError;
    fn try_from(
        value: protogen::gen::metastore::service::CreateCredentials,
    ) -> Result<Self, Self::Error> {
        let options = value
            .options
            .ok_or(ProtoConvError::RequiredField("options".to_string()))?;

        Ok(Self {
            name: value.name,
            options: options.try_into()?,
            comment: value.comment,
        })
    }
}

impl UserDefinedLogicalNodeCore for CreateCredentials {
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
        write!(f, "CreateCredentials")
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl ExtensionType for CreateCredentials {
    const EXTENSION_NAME: &'static str = "CreateCredentials";
    fn try_decode_extension(extension: &LogicalPlanExtension) -> Result<Self> {
        match extension.node.as_any().downcast_ref::<Self>() {
            Some(s) => Ok(s.clone()),
            None => Err(internal!("CreateCredentials::try_decode_extension failed",)),
        }
    }

    fn try_encode(&self, buf: &mut Vec<u8>, _codec: &dyn LogicalExtensionCodec) -> Result<()> {
        use ::protogen::sqlexec::logical_plan::{LogicalPlanExtension, LogicalPlanExtensionType};

        use protogen::gen::metastore::service as protogen;
        let Self {
            name,
            options,
            comment,
        } = self.clone();

        let proto = protogen::CreateCredentials {
            name,
            options: Some(options.into()),
            comment,
        };
        let plan_type = LogicalPlanExtensionType::CreateCredentials(proto);

        let lp_extension = LogicalPlanExtension {
            inner: Some(plan_type),
        };

        lp_extension
            .encode(buf)
            .map_err(|e| internal!("{}", e.to_string()))?;

        Ok(())
    }
}
