use rayexec_bullet::datatype::{DataType, DecimalTypeMeta};
use rayexec_bullet::field::{Field, Schema};

pub const TPCH_TABLES: &[TableInfo] = &[
    CUSTOMER_INFO,
    LINEITEM_INFO,
    NATION_INFO,
    ORDERS_INFO,
    PARTSUPP_INFO,
    PART_INFO,
    REGION_INFO,
    SUPPLIER_INFO,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableInfo {
    pub name: &'static str,
    pub column_names: &'static [&'static str],
    pub column_types: &'static [DataType],
}

impl TableInfo {
    pub fn schema(&self) -> Schema {
        Schema::new(
            self.column_names
                .iter()
                .zip(self.column_types)
                .map(|(name, typ)| Field::new(*name, typ.clone(), true)),
        )
    }
}

pub const CUSTOMER_INFO: TableInfo = TableInfo {
    name: "customer",
    column_names: &[
        "c_custkey",
        "c_name",
        "c_address",
        "c_nationkey",
        "c_phone",
        "c_acctbal",
        "c_mktsegment",
        "c_comment",
    ],
    column_types: &[
        DataType::Int32,
        DataType::Utf8,
        DataType::Utf8,
        DataType::Int32,
        DataType::Utf8,
        DataType::Decimal64(DecimalTypeMeta::new(15, 2)),
        DataType::Utf8,
        DataType::Utf8,
    ],
};

pub const LINEITEM_INFO: TableInfo = TableInfo {
    name: "lineitem",
    column_names: &[
        "l_orderkey",
        "l_partkey",
        "l_suppkey",
        "l_linenumber",
        "l_quantity",
        "l_extendedprice",
        "l_discount",
        "l_tax",
        "l_returnflag",
        "l_linestatus",
        "l_shipdate",
        "l_commitdate",
        "l_receiptdate",
        "l_shipinstruct",
        "l_shipmode",
        "l_comment",
    ],
    column_types: &[
        DataType::Int32,
        DataType::Int32,
        DataType::Int32,
        DataType::Int32,
        DataType::Decimal64(DecimalTypeMeta::new(15, 2)),
        DataType::Decimal64(DecimalTypeMeta::new(15, 2)),
        DataType::Decimal64(DecimalTypeMeta::new(15, 2)),
        DataType::Decimal64(DecimalTypeMeta::new(15, 2)),
        DataType::Utf8,
        DataType::Utf8,
        DataType::Date32,
        DataType::Date32,
        DataType::Date32,
        DataType::Utf8,
        DataType::Utf8,
        DataType::Utf8,
    ],
};

pub const NATION_INFO: TableInfo = TableInfo {
    name: "nation",
    column_names: &["n_nationkey", "n_name", "n_regionkey", "n_comment"],
    column_types: &[
        DataType::Int32,
        DataType::Utf8,
        DataType::Int32,
        DataType::Utf8,
    ],
};

pub const ORDERS_INFO: TableInfo = TableInfo {
    name: "orders",
    column_names: &[
        "o_orderkey",
        "o_custkey",
        "o_orderstatus",
        "o_totalprice",
        "o_orderdate",
        "o_orderpriority",
        "o_clerk",
        "o_shippriority",
        "o_comment",
    ],
    column_types: &[
        DataType::Int32,
        DataType::Int32,
        DataType::Utf8,
        DataType::Decimal64(DecimalTypeMeta::new(15, 2)),
        DataType::Date32,
        DataType::Utf8,
        DataType::Utf8,
        DataType::Int32,
        DataType::Utf8,
    ],
};

pub const PART_INFO: TableInfo = TableInfo {
    name: "part",
    column_names: &[
        "p_partkey",
        "p_name",
        "p_mfgr",
        "p_brand",
        "p_type",
        "p_size",
        "p_container",
        "p_retailprice",
        "p_comment",
    ],
    column_types: &[
        DataType::Int32,
        DataType::Utf8,
        DataType::Utf8,
        DataType::Utf8,
        DataType::Utf8,
        DataType::Int32,
        DataType::Utf8,
        DataType::Decimal64(DecimalTypeMeta::new(15, 2)),
        DataType::Utf8,
    ],
};

pub const PARTSUPP_INFO: TableInfo = TableInfo {
    name: "partsupp",
    column_names: &[
        "ps_partkey",
        "ps_suppkey",
        "ps_availqty",
        "ps_supplycost",
        "ps_comment",
    ],
    column_types: &[
        DataType::Int32,
        DataType::Int32,
        DataType::Int32,
        DataType::Decimal64(DecimalTypeMeta::new(15, 2)),
        DataType::Utf8,
    ],
};

pub const REGION_INFO: TableInfo = TableInfo {
    name: "region",
    column_names: &["r_regionkey", "r_name", "r_comment"],
    column_types: &[DataType::Int32, DataType::Utf8, DataType::Utf8],
};

pub const SUPPLIER_INFO: TableInfo = TableInfo {
    name: "supplier",
    column_names: &[
        "s_suppkey",
        "s_name",
        "s_address",
        "s_nationkey",
        "s_phone",
        "s_acctbal",
        "s_comment",
    ],
    column_types: &[
        DataType::Int32,
        DataType::Utf8,
        DataType::Utf8,
        DataType::Int32,
        DataType::Utf8,
        DataType::Decimal64(DecimalTypeMeta::new(15, 2)),
        DataType::Utf8,
    ],
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_len_eq_types_len() {
        for tbl in TPCH_TABLES {
            assert_eq!(tbl.column_names.len(), tbl.column_types.len());
        }
    }
}
