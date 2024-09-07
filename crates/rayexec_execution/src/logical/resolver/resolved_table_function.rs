use crate::{
    database::DatabaseContext,
    functions::table::{PlannedTableFunction, TableFunctionArgs},
    proto::DatabaseProtoConv,
};
use rayexec_error::{OptionExt, Result};
use rayexec_parser::ast;
use rayexec_proto::ProtoConv;

/// A resolved table function reference.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedTableFunctionReference {
    /// Name of the original function.
    ///
    /// This is used to allow the user to reference the output of the function
    /// if not provided an alias.
    pub name: String,
    /// The function.
    pub func: Box<dyn PlannedTableFunction>,
    // TODO: Maybe keep args here?
}

impl DatabaseProtoConv for ResolvedTableFunctionReference {
    type ProtoType = rayexec_proto::generated::resolver::ResolvedTableFunctionReference;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            name: self.name.clone(),
            func: Some(self.func.to_proto_ctx(context)?),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            name: proto.name,
            func: DatabaseProtoConv::from_proto_ctx(proto.func.required("func")?, context)?,
        })
    }
}

/// An unresolved reference to a table function.
#[derive(Debug, Clone, PartialEq)]
pub struct UnresolvedTableFunctionReference {
    /// Original reference in the ast.
    pub reference: ast::ObjectReference,
    /// Arguments to the function.
    ///
    /// Note that these are required to be constant and so we don't need to
    /// delay binding.
    pub args: TableFunctionArgs,
}

impl ProtoConv for UnresolvedTableFunctionReference {
    type ProtoType = rayexec_proto::generated::resolver::UnresolvedTableFunctionReference;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            reference: Some(self.reference.to_proto()?),
            args: Some(self.args.to_proto()?),
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {
            reference: ast::ObjectReference::from_proto(proto.reference.required("reference")?)?,
            args: TableFunctionArgs::from_proto(proto.args.required("args")?)?,
        })
    }
}
