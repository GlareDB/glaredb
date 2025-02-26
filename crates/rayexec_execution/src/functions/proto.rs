use std::collections::HashMap;

use rayexec_error::{OptionExt, Result};
use rayexec_proto::ProtoConv;

use super::aggregate::{AggregateFunction2, PlannedAggregateFunction2};
use super::copy::{CopyToArgs, CopyToFunction};
use super::table::{PlannedTableFunction, TableFunction};
use crate::arrays::scalar::ScalarValue;
use crate::database::catalog::CatalogTx;
use crate::database::DatabaseContext;
use crate::proto::DatabaseProtoConv;

pub const FUNCTION_LOOKUP_CATALOG: &str = "glare_catalog";

impl DatabaseProtoConv for Box<dyn AggregateFunction2> {
    type ProtoType = rayexec_proto::generated::functions::AggregateFunction;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            name: self.name().to_string(),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        let tx = &CatalogTx {};
        let ent = context
            .system_catalog()?
            .get_schema(tx, FUNCTION_LOOKUP_CATALOG)?
            .required("lookup schema")?
            .get_aggregate_function(tx, &proto.name)?
            .required("agg function")?;
        let ent = ent.try_as_aggregate_function_entry()?;

        Ok(ent.function.clone())
    }
}

impl DatabaseProtoConv for PlannedAggregateFunction2 {
    type ProtoType = rayexec_proto::generated::functions::PlannedAggregateFunction;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        unimplemented!()
        // let mut state = Vec::new();
        // self.encode_state(&mut state)?;

        // Ok(Self::ProtoType {
        //     name: self.aggregate_function().name().to_string(),
        //     state,
        // })
    }

    fn from_proto_ctx(_proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        unimplemented!()
        // let tx = &CatalogTx {};
        // let ent = context
        //     .system_catalog()?
        //     .get_schema(tx, FUNCTION_LOOKUP_CATALOG)?
        //     .required("lookup schema")?
        //     .get_aggregate_function(tx, &proto.name)?
        //     .required("agg function")?;
        // let ent = ent.try_as_aggregate_function_entry()?;

        // let planned = ent.function.decode_state(&proto.state)?;

        // Ok(planned)
    }
}

impl DatabaseProtoConv for Box<dyn TableFunction> {
    type ProtoType = rayexec_proto::generated::functions::TableFunction;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            name: self.name().to_string(),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        let tx = &CatalogTx {};
        let ent = context
            .system_catalog()?
            .get_schema(tx, FUNCTION_LOOKUP_CATALOG)?
            .required("lookup schema")?
            .get_table_function(tx, &proto.name)?
            .required("table function")?;
        let ent = ent.try_as_table_function_entry()?;

        Ok(ent.function.clone())
    }
}

impl DatabaseProtoConv for PlannedTableFunction {
    type ProtoType = rayexec_proto::generated::functions::PlannedTableFunction;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        unimplemented!()
        // let mut state = Vec::new();
        // self.encode_state(&mut state)?;

        // Ok(Self::ProtoType {
        //     name: self.table_function().name().to_string(),
        //     state,
        // })
    }

    fn from_proto_ctx(_proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        unimplemented!()
        // let tx = &CatalogTx {};
        // let ent = context
        //     .system_catalog()?
        //     .get_schema(tx, FUNCTION_LOOKUP_CATALOG)?
        //     .required("lookup schema")?
        //     .get_table_function(tx, &proto.name)?
        //     .required("table function")?;
        // let ent = ent.try_as_table_function_entry()?;

        // let planned = ent.function.decode_state(&proto.state)?;

        // Ok(planned)
    }
}

impl DatabaseProtoConv for Box<dyn CopyToFunction> {
    type ProtoType = rayexec_proto::generated::functions::CopyToFunction;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            name: self.name().to_string(),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        let tx = &CatalogTx {};
        let ent = context
            .system_catalog()?
            .get_schema(tx, FUNCTION_LOOKUP_CATALOG)?
            .required("lookup schema")?
            .get_copy_to_function(tx, &proto.name)?
            .required("table function")?;
        let ent = ent.try_as_copy_to_function_entry()?;

        Ok(ent.function.clone())
    }
}

impl ProtoConv for CopyToArgs {
    type ProtoType = rayexec_proto::generated::functions::CopyToFunctionArgs;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        let mut named = HashMap::new();
        for (key, val) in &self.named {
            named.insert(key.clone(), val.to_proto()?);
        }

        Ok(Self::ProtoType { named })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        let mut named = HashMap::new();
        for (key, val) in proto.named {
            named.insert(key, ScalarValue::from_proto(val)?);
        }

        Ok(Self { named })
    }
}
