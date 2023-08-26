use super::*;

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct CopyTo {
    pub source: DfLogicalPlan,
    pub dest: CopyToDestinationOptions,
    pub format: CopyToFormatOptions,
}

impl std::fmt::Debug for CopyTo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CopyTo")
            .field("source", &self.source.schema())
            .field("dest", &self.dest)
            .field("format", &self.format)
            .finish()
    }
}

impl UserDefinedLogicalNodeCore for CopyTo {
    fn name(&self) -> &str {
        Self::EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&DfLogicalPlan> {
        vec![&self.source]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &GENERIC_OPERATION_AND_COUNT_LOGICAL_SCHEMA
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CopyTo")
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        println!("CopyTo from_template");
        self.clone()
    }
}
impl ExtensionNode for CopyTo {
    type ProtoRepr = protogen::sqlexec::logical_plan::CopyTo;
    const EXTENSION_NAME: &'static str = "CopyTo";
    fn try_decode(
        proto: Self::ProtoRepr,
        ctx: &SessionContext,
        codec: &dyn LogicalExtensionCodec,
    ) -> std::result::Result<Self, ProtoConvError> {
        let source = proto
            .source
            .map(|src| src.try_into_logical_plan(ctx, codec))
            .ok_or_else(|| ProtoConvError::RequiredField("source".to_string()))?
            .map_err(ProtoConvError::DataFusionError)?;
        let dest = proto
            .dest
            .ok_or_else(|| ProtoConvError::RequiredField("dest".to_string()))?;
        let format = proto
            .format
            .ok_or_else(|| ProtoConvError::RequiredField("format".to_string()))?;

        Ok(Self {
            source,
            dest: dest.try_into()?,
            format: format.try_into()?,
        })
    }

    fn try_decode_extension(extension: &LogicalPlanExtension) -> Result<Self> {
        match extension.node.as_any().downcast_ref::<Self>() {
            Some(s) => Ok(s.clone()),
            None => Err(internal!(
                "CopyTo::try_from_extension: unsupported extension",
            )),
        }
    }

    fn try_encode(&self, buf: &mut Vec<u8>, codec: &dyn LogicalExtensionCodec) -> Result<()> {
        use protogen::sqlexec::logical_plan as protogen;

        let source = LogicalPlanNode::try_from_logical_plan(&self.source, codec)
            .map_err(|e| internal!("unable to encode source: {}", e.to_string()))?;

        let dest = self.dest.clone().try_into()?;
        let format = self.format.clone().try_into()?;
        let proto = protogen::CopyTo {
            source: Some(source),
            dest: Some(dest),
            format: Some(format),
        };

        let extension = protogen::LogicalPlanExtensionType::CopyTo(proto);

        let lp_extension = protogen::LogicalPlanExtension {
            inner: Some(extension),
        };

        lp_extension
            .encode(buf)
            .map_err(|e| internal!("{}", e.to_string()))?;

        Ok(())
    }
}
