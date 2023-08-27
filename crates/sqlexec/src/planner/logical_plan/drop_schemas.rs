use super::*;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct DropSchemas {
    pub references: Vec<OwnedFullSchemaReference>,
    pub if_exists: bool,
    pub cascade: bool,
}

impl UserDefinedLogicalNodeCore for DropSchemas {
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
        write!(f, "DropSchemas")
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl ExtensionNode for DropSchemas {
    type ProtoRepr = protogen::sqlexec::logical_plan::DropSchemas;
    const EXTENSION_NAME: &'static str = "DropSchemas";
    fn try_decode(
        proto: Self::ProtoRepr,
        _ctx: &SessionContext,
        _codec: &dyn LogicalExtensionCodec,
    ) -> std::result::Result<Self, ProtoConvError> {
        let references = proto
            .references
            .into_iter()
            .map(|r| r.into())
            .collect::<Vec<_>>();

        Ok(Self {
            references,
            if_exists: proto.if_exists,
            cascade: proto.cascade,
        })
    }
    fn try_decode_extension(extension: &LogicalPlanExtension) -> Result<Self> {
        match extension.node.as_any().downcast_ref::<Self>() {
            Some(s) => Ok(s.clone()),
            None => Err(internal!("DropSchemas::try_decode_extension failed",)),
        }
    }

    fn try_encode(&self, buf: &mut Vec<u8>, _codec: &dyn LogicalExtensionCodec) -> Result<()> {
        use protogen::sqlexec::logical_plan as protogen;
        let references = self
            .references
            .clone()
            .into_iter()
            .map(|r| r.into())
            .collect::<Vec<_>>();

        let create_schema = protogen::DropSchemas {
            references,
            if_exists: self.if_exists,
            cascade: self.cascade,
        };
        let plan_type = protogen::LogicalPlanExtensionType::DropSchemas(create_schema);

        let lp_extension = protogen::LogicalPlanExtension {
            inner: Some(plan_type),
        };

        lp_extension
            .encode(buf)
            .map_err(|e| internal!("{}", e.to_string()))?;

        Ok(())
    }
}
