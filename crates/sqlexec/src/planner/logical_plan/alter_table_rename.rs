use super::*;
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct AlterTableRename {
    pub tbl_reference: OwnedFullObjectReference,
    pub new_tbl_reference: OwnedFullObjectReference,
}
impl UserDefinedLogicalNodeCore for AlterTableRename {
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

impl ExtensionNode for AlterTableRename {
    type ProtoRepr = protogen::sqlexec::logical_plan::AlterTableRename;
    const EXTENSION_NAME: &'static str = "AlterTableRename";
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

        let new_reference = proto
            .new_reference
            .ok_or(ProtoConvError::RequiredField(
                "new_name is required".to_string(),
            ))?
            .into();

        Ok(Self {
            tbl_reference: reference,
            new_tbl_reference: new_reference,
        })
    }

    fn try_downcast_extension(extension: &LogicalPlanExtension) -> Result<Self> {
        match extension.node.as_any().downcast_ref::<Self>() {
            Some(s) => Ok(s.clone()),
            None => Err(internal!(
                "AlterTableRename::try_from_extension: unsupported extension",
            )),
        }
    }

    fn try_encode(&self, buf: &mut Vec<u8>, _codec: &dyn LogicalExtensionCodec) -> Result<()> {
        use protogen::sqlexec::logical_plan as protogen;

        let reference = self.tbl_reference.clone().into();
        let new_reference = self.new_tbl_reference.clone().into();

        let alter_table = protogen::AlterTableRename {
            reference: Some(reference),
            new_reference: Some(new_reference),
        };

        let extension = protogen::LogicalPlanExtensionType::AlterTableRename(alter_table);

        let lp_extension = protogen::LogicalPlanExtension {
            inner: Some(extension),
        };

        lp_extension
            .encode(buf)
            .map_err(|e| internal!("{}", e.to_string()))?;

        Ok(())
    }
}
