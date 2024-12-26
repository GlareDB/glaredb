use std::collections::HashMap;

use crate::arrays::scalar::OwnedScalarValue;
use rayexec_error::Result;
use rayexec_parser::ast;
use rayexec_proto::ProtoConv;

use crate::database::DatabaseContext;
use crate::functions::table::{PlannedTableFunction, TableFunction};
use crate::proto::DatabaseProtoConv;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConstantFunctionArgs {
    pub positional: Vec<OwnedScalarValue>,
    pub named: HashMap<String, OwnedScalarValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedTableFunctionReference {
    /// Scan table functions can be fully resolved as their arguments are
    /// constant.
    Scan(PlannedTableFunction),
    /// We have a function that's an in/out funciton, but we need to wait until
    /// we're in the binding phase before planning this as it requires knowledge
    /// of its inputs.
    InOut(Box<dyn TableFunction>),
}

impl ResolvedTableFunctionReference {
    pub fn base_table_alias(&self) -> String {
        match self {
            ResolvedTableFunctionReference::Scan(func) => func.function.name().to_string(),
            ResolvedTableFunctionReference::InOut(func) => func.name().to_string(),
        }
    }
}

impl DatabaseProtoConv for ResolvedTableFunctionReference {
    type ProtoType = rayexec_proto::generated::resolver::ResolvedTableFunctionReference;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        unimplemented!()
        // Ok(Self::ProtoType {
        //     name: self.name.clone(),
        //     func: Some(self.func.to_proto_ctx(context)?),
        // })
    }

    fn from_proto_ctx(_proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        unimplemented!()
        // Ok(Self {
        //     name: proto.name,
        //     func: DatabaseProtoConv::from_proto_ctx(proto.func.required("func")?, context)?,
        // })
    }
}

/// An unresolved reference to a table function.
#[derive(Debug, Clone, PartialEq)]
pub struct UnresolvedTableFunctionReference {
    /// Original reference in the ast.
    pub reference: ast::ObjectReference,
    /// Constant arguments to the function.
    ///
    /// This currently assumes that any unresolved table function is for trying
    /// to do a scan on a data source that's not registered (e.g.
    /// `read_postgres` from wasm).
    // TODO: Optionally set this? There's a possibility that the remote side has
    // an in/out function that we don't know about, and so these args aren't
    // actually needed. Not urgent to figure out right now.
    pub args: ConstantFunctionArgs,
}

impl ProtoConv for UnresolvedTableFunctionReference {
    type ProtoType = rayexec_proto::generated::resolver::UnresolvedTableFunctionReference;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        unimplemented!()
        // Ok(Self::ProtoType {
        //     reference: Some(self.reference.to_proto()?),
        //     args: Some(self.args.to_proto()?),
        // })
    }

    fn from_proto(_proto: Self::ProtoType) -> Result<Self> {
        unimplemented!()
        // Ok(Self {
        //     reference: ast::ObjectReference::from_proto(proto.reference.required("reference")?)?,
        //     args: TableFunctionInputs::from_proto(proto.args.required("args")?)?,
        // })
    }
}
