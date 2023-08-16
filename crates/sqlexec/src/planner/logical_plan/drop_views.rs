use super::*;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct DropViews {
    pub names: Vec<OwnedTableReference>,
    pub if_exists: bool,
}

impl TryFrom<protogen::sqlexec::logical_plan::DropViews> for DropViews {
    type Error = ProtoConvError;

    fn try_from(proto: protogen::sqlexec::logical_plan::DropViews) -> Result<Self, Self::Error> {
        let names = proto
            .names
            .into_iter()
            .map(|name| name.try_into())
            .collect::<Result<_, _>>()?;

        Ok(Self {
            names,
            if_exists: proto.if_exists,
        })
    }
}

impl UserDefinedLogicalNodeCore for DropViews {
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
        write!(f, "DropViews")
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl ExtensionNode for DropViews {
    const EXTENSION_NAME: &'static str = "DropViews";
    fn try_decode_extension(extension: &LogicalPlanExtension) -> Result<Self> {
        match extension.node.as_any().downcast_ref::<Self>() {
            Some(s) => Ok(s.clone()),
            None => Err(internal!(
                "DropViews::try_decode_extension: unsupported extension",
            )),
        }
    }

    fn try_encode(&self, buf: &mut Vec<u8>, _codec: &dyn LogicalExtensionCodec) -> Result<()> {
        use protogen::sqlexec::logical_plan as protogen;
        let names = self
            .names
            .iter()
            .map(|name| name.to_owned_reference().into())
            .collect::<Vec<_>>();

        let drop_tables = protogen::DropViews {
            names,
            if_exists: self.if_exists,
        };
        let plan_type = protogen::LogicalPlanExtensionType::DropViews(drop_tables);

        let lp_extension = protogen::LogicalPlanExtension {
            inner: Some(plan_type),
        };

        lp_extension
            .encode(buf)
            .map_err(|e| internal!("{}", e.to_string()))?;

        Ok(())
    }
}
