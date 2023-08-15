use super::*;
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct AlterTableRename {
    pub name: OwnedTableReference,
    pub new_name: OwnedTableReference,
}

impl TryFrom<protogen::sqlexec::logical_plan::AlterTableRename> for AlterTableRename {
    type Error = ProtoConvError;

    fn try_from(
        proto: protogen::sqlexec::logical_plan::AlterTableRename,
    ) -> Result<Self, Self::Error> {
        let name = proto
            .name
            .ok_or(ProtoConvError::RequiredField(
                "name is required".to_string(),
            ))?
            .try_into()?;

        let new_name = proto
            .new_name
            .ok_or(ProtoConvError::RequiredField(
                "new_name is required".to_string(),
            ))?
            .try_into()?;

        Ok(Self { name, new_name })
    }
}

impl UserDefinedLogicalNodeCore for AlterTableRename {
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
    const EXTENSION_NAME: &'static str = "AlterTableRename";

    fn try_decode_extension(extension: &LogicalPlanExtension) -> Result<Self> {
        match extension.node.as_any().downcast_ref::<Self>() {
            Some(s) => Ok(s.clone()),
            None => Err(internal!(
                "AlterTableRename::try_from_extension: unsupported extension",
            )),
        }
    }

    fn try_encode(&self, buf: &mut Vec<u8>, _codec: &dyn LogicalExtensionCodec) -> Result<()> {
        use protogen::sqlexec::logical_plan as protogen;

        let name = self.name.clone().into();
        let new_name = self.new_name.clone().into();

        let alter_table = protogen::AlterTableRename {
            name: Some(name),
            new_name: Some(new_name),
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
