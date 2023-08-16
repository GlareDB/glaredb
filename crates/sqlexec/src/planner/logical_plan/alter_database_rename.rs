use datafusion::prelude::SessionContext;

use super::*;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct AlterDatabaseRename {
    pub name: String,
    pub new_name: String,
}

impl UserDefinedLogicalNodeCore for AlterDatabaseRename {
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

impl ExtensionNode for AlterDatabaseRename {
    type ProtoRepr = protogen::gen::metastore::service::AlterDatabaseRename;
    const EXTENSION_NAME: &'static str = "AlterDatabaseRename";
    fn try_decode(
        proto: Self::ProtoRepr,
        _ctx: &SessionContext,
        _codec: &dyn LogicalExtensionCodec,
    ) -> std::result::Result<Self, ProtoConvError> {
        Ok(Self {
            name: proto.name,
            new_name: proto.new_name,
        })
    }
    fn try_decode_extension(extension: &LogicalPlanExtension) -> Result<Self> {
        match extension.node.as_any().downcast_ref::<Self>() {
            Some(s) => Ok(s.clone()),
            None => Err(internal!(
                "AlterDatabaseRename::try_from_extension: unsupported extension",
            )),
        }
    }

    fn try_encode(&self, buf: &mut Vec<u8>, _codec: &dyn LogicalExtensionCodec) -> Result<()> {
        use protogen::sqlexec::logical_plan as protogen;
        let Self { name, new_name } = self.clone();

        let alter_table =
            ::protogen::gen::metastore::service::AlterDatabaseRename { name, new_name };

        let extension = protogen::LogicalPlanExtensionType::AlterDatabaseRename(alter_table);

        let lp_extension = protogen::LogicalPlanExtension {
            inner: Some(extension),
        };

        lp_extension
            .encode(buf)
            .map_err(|e| internal!("{}", e.to_string()))?;

        Ok(())
    }
}
