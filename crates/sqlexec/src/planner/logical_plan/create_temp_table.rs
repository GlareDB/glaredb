use super::*;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CreateTempTable {
    pub reference: OwnedFullObjectReference,
    pub if_not_exists: bool,
    pub schema: DFSchemaRef,
    pub source: Option<DfLogicalPlan>,
}

impl UserDefinedLogicalNodeCore for CreateTempTable {
    fn name(&self) -> &str {
        Self::EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&DfLogicalPlan> {
        match self.source {
            Some(ref src) => vec![src],
            None => vec![],
        }
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &GENERIC_OPERATION_LOGICAL_SCHEMA
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", Self::EXTENSION_NAME)
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl ExtensionNode for CreateTempTable {
    type ProtoRepr = protogen::sqlexec::logical_plan::CreateTempTable;
    const EXTENSION_NAME: &'static str = "CreateTempTable";
    fn try_decode(
        proto: Self::ProtoRepr,
        ctx: &SessionContext,
        codec: &dyn LogicalExtensionCodec,
    ) -> std::result::Result<Self, ProtoConvError> {
        let reference = proto
            .reference
            .ok_or(ProtoConvError::RequiredField(
                "reference is required".to_string(),
            ))?
            .into();
        let schema = proto
            .schema
            .ok_or(ProtoConvError::RequiredField(
                "schema name is required".to_string(),
            ))?
            .try_into()?;
        let source = proto
            .source
            .map(|src| src.try_into_logical_plan(ctx, codec))
            .transpose()
            .map_err(ProtoConvError::DataFusionError)?;

        Ok(Self {
            reference,
            if_not_exists: proto.if_not_exists,
            schema,
            source,
        })
    }
    fn try_decode_extension(extension: &LogicalPlanExtension) -> Result<Self> {
        match extension.node.as_any().downcast_ref::<Self>() {
            Some(s) => Ok(s.clone()),
            None => Err(internal!(
                "CreateTempTable::try_from_extension: unsupported extension",
            )),
        }
    }

    fn try_encode(&self, buf: &mut Vec<u8>, codec: &dyn LogicalExtensionCodec) -> Result<()> {
        use protogen::sqlexec::logical_plan as protogen;
        let schema = &self.schema;

        let schema: Option<datafusion_proto::protobuf::DfSchema> = schema.try_into().ok();

        let source = self.source.as_ref().map(|src| {
            LogicalPlanNode::try_from_logical_plan(src, codec)
                .map_err(|e| internal!("unable to encode source: {}", e.to_string()))
                .unwrap()
        });

        let create_table = protogen::CreateTempTable {
            reference: Some(self.reference.clone().into()),
            if_not_exists: self.if_not_exists,
            schema,
            source,
        };

        let extension = protogen::LogicalPlanExtensionType::CreateTempTable(create_table);

        let lp_extension = protogen::LogicalPlanExtension {
            inner: Some(extension),
        };

        lp_extension
            .encode(buf)
            .map_err(|e| internal!("{}", e.to_string()))?;

        Ok(())
    }
}
