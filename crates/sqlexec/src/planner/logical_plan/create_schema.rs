use super::*;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CreateSchema {
    pub schema_reference: OwnedFullSchemaReference,
    pub if_not_exists: bool,
}

impl UserDefinedLogicalNodeCore for CreateSchema {
    fn name(&self) -> &str {
        Self::EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&DfLogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &GENERIC_OPERATION_LOGICAL_SCHEMA
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CreateSchema")
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl ExtensionNode for CreateSchema {
    type ProtoRepr = protogen::sqlexec::logical_plan::CreateSchema;
    const EXTENSION_NAME: &'static str = "CreateSchema";
    fn try_decode(
        proto: Self::ProtoRepr,
        _ctx: &SessionContext,
        _codec: &dyn LogicalExtensionCodec,
    ) -> std::result::Result<Self, ProtoConvError> {
        let reference = proto
            .reference
            .ok_or(ProtoConvError::RequiredField(
                "reference is required".to_string(),
            ))?
            .into();

        Ok(Self {
            schema_reference: reference,
            if_not_exists: proto.if_not_exists,
        })
    }
    fn try_downcast_extension(extension: &LogicalPlanExtension) -> Result<Self> {
        match extension.node.as_any().downcast_ref::<Self>() {
            Some(s) => Ok(s.clone()),
            None => Err(internal!("CreateSchema::try_decode_extension failed",)),
        }
    }

    fn try_encode(&self, buf: &mut Vec<u8>, _codec: &dyn LogicalExtensionCodec) -> Result<()> {
        use protogen::sqlexec::logical_plan as protogen;

        let create_schema = protogen::CreateSchema {
            reference: Some(self.schema_reference.clone().into()),
            if_not_exists: self.if_not_exists,
        };
        let plan_type = protogen::LogicalPlanExtensionType::CreateSchema(create_schema);

        let lp_extension = protogen::LogicalPlanExtension {
            inner: Some(plan_type),
        };

        lp_extension
            .encode(buf)
            .map_err(|e| internal!("{}", e.to_string()))?;

        Ok(())
    }
}
