use std::collections::HashMap;

use crate::{
    database::{catalog::CatalogTx, DatabaseContext},
    proto::DatabaseProtoConv,
};
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{OptionExt, Result};
use rayexec_proto::ProtoConv;

use super::{
    aggregate::{AggregateFunction, PlannedAggregateFunction},
    scalar::{PlannedScalarFunction, ScalarFunction},
    table::{PlannedTableFunction, TableFunction, TableFunctionArgs},
};

const LOOKUP_CATALOG: &str = "glare_catalog";

impl DatabaseProtoConv for Box<dyn ScalarFunction> {
    type ProtoType = rayexec_proto::generated::expr::ScalarFunction;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            name: self.name().to_string(),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        let tx = &CatalogTx {};
        let scalar = context
            .system_catalog()?
            .get_scalar_fn(tx, LOOKUP_CATALOG, &proto.name)?
            .required("scalar function")?;

        Ok(scalar)
    }
}

impl DatabaseProtoConv for Box<dyn PlannedScalarFunction> {
    type ProtoType = rayexec_proto::generated::expr::PlannedScalarFunction;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        let mut state = Vec::new();
        self.encode_state(&mut state)?;

        Ok(Self::ProtoType {
            name: self.scalar_function().name().to_string(),
            state,
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        let tx = &CatalogTx {};
        let scalar = context
            .system_catalog()?
            .get_scalar_fn(tx, LOOKUP_CATALOG, &proto.name)?
            .required("scalar function")?;

        let planned = scalar.decode_state(&proto.state)?;

        Ok(planned)
    }
}

impl DatabaseProtoConv for Box<dyn AggregateFunction> {
    type ProtoType = rayexec_proto::generated::expr::AggregateFunction;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            name: self.name().to_string(),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        let tx = &CatalogTx {};
        let agg = context
            .system_catalog()?
            .get_aggregate_fn(tx, LOOKUP_CATALOG, &proto.name)?
            .required("aggregate function")?;

        Ok(agg)
    }
}

impl DatabaseProtoConv for Box<dyn PlannedAggregateFunction> {
    type ProtoType = rayexec_proto::generated::expr::PlannedAggregateFunction;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        let mut state = Vec::new();
        self.encode_state(&mut state)?;

        Ok(Self::ProtoType {
            name: self.aggregate_function().name().to_string(),
            state,
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        let tx = &CatalogTx {};
        let agg = context
            .system_catalog()?
            .get_aggregate_fn(tx, LOOKUP_CATALOG, &proto.name)?
            .required("aggregate function")?;

        let planned = agg.decode_state(&proto.state)?;

        Ok(planned)
    }
}

impl DatabaseProtoConv for Box<dyn TableFunction> {
    type ProtoType = rayexec_proto::generated::expr::TableFunction;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            name: self.name().to_string(),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        let tx = &CatalogTx {};
        let table = context
            .system_catalog()?
            .get_table_fn(tx, LOOKUP_CATALOG, &proto.name)?
            .required("table function")?;

        Ok(table)
    }
}

impl DatabaseProtoConv for Box<dyn PlannedTableFunction> {
    type ProtoType = rayexec_proto::generated::expr::PlannedTableFunction;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        let mut state = Vec::new();
        self.encode_state(&mut state)?;

        Ok(Self::ProtoType {
            name: self.table_function().name().to_string(),
            state,
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        let tx = &CatalogTx {};
        let table = context
            .system_catalog()?
            .get_table_fn(tx, LOOKUP_CATALOG, &proto.name)?
            .required("table function")?;

        let planned = table.decode_state(&proto.state)?;

        Ok(planned)
    }
}

impl ProtoConv for TableFunctionArgs {
    type ProtoType = rayexec_proto::generated::expr::TableFunctionArgs;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        let mut named = HashMap::new();
        for (key, val) in &self.named {
            named.insert(key.clone(), val.to_proto()?);
        }

        Ok(Self::ProtoType {
            named,
            positional: self
                .positional
                .iter()
                .map(|v| v.to_proto())
                .collect::<Result<Vec<_>>>()?,
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        let mut named = HashMap::new();
        for (key, val) in proto.named {
            named.insert(key, OwnedScalarValue::from_proto(val)?);
        }

        Ok(Self {
            named,
            positional: proto
                .positional
                .into_iter()
                .map(OwnedScalarValue::from_proto)
                .collect::<Result<Vec<_>>>()?,
        })
    }
}
