//! A small cli for reading in (TPC-H) sql queries and outputting the logical plan.
//!
//! This exmaple can be ran via `cargo run --example logicalplan FILENAME` where
//! the provided file consists of one or more sql queries.
use coretypes::datatype::{DataType, NullableType, RelationSchema};
use sqlparser::ast;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use sqlrel::catalog::{Catalog, CatalogError, ResolvedTableReference, TableReference, TableSchema};
use sqlrel::planner::Planner;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let filename = std::env::args().nth(1).ok_or("missing filename")?;

    println!("Parsing from '{}'", filename);
    let contents = std::fs::read_to_string(&filename)?;
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, &contents)?;

    let catalog = TpchCatalog::new();
    let planner = Planner::new(&catalog);

    for statement in statements.into_iter() {
        match statement {
            ast::Statement::Query(query) => {
                let plan = planner.plan_query(*query)?;
                println!("Plan:\n{}\n", plan);
            }
            _ => Err("unsupported statement")?,
        };
    }

    Ok(())
}

/// Helper for creating tables.
///
/// All columns will be implicitly nullable.
fn create_table(name: &str, cols: Vec<(&str, DataType)>) -> TableSchema {
    let resolved = ResolvedTableReference {
        catalog: "db".to_string(),
        schema: "tpch".to_string(),
        base: name.to_string(),
    };

    let mut names = Vec::new();
    let mut types = Vec::new();
    for col in cols.into_iter() {
        names.push(col.0.to_string());
        types.push(NullableType::new_nullable(col.1));
    }

    let schema = RelationSchema::new(types);
    TableSchema::new(resolved, names, schema).unwrap()
}

#[derive(Debug)]
struct TpchCatalog {
    tables: Vec<TableSchema>,
}

impl TpchCatalog {
    fn new() -> TpchCatalog {
        let tables = vec![
            create_table(
                "part",
                vec![
                    ("p_partkey", DataType::Int64),
                    ("p_name", DataType::Utf8),
                    ("p_mfgr", DataType::Utf8),
                    ("p_brand", DataType::Utf8),
                    ("p_type", DataType::Utf8),
                    ("p_size", DataType::Int32),
                    ("p_container", DataType::Utf8),
                    ("p_retailprice", DataType::Float64),
                    ("p_comment", DataType::Utf8),
                ],
            ),
            create_table(
                "supplier",
                vec![
                    ("s_suppkey", DataType::Int64),
                    ("s_name", DataType::Utf8),
                    ("s_address", DataType::Utf8),
                    ("s_nationkey", DataType::Int64),
                    ("s_acctbal", DataType::Float64),
                    ("s_comment", DataType::Utf8),
                ],
            ),
            create_table(
                "partsupp",
                vec![
                    ("ps_partkey", DataType::Int64),
                    ("ps_suppkey", DataType::Int64),
                    ("ps_availqty", DataType::Int32),
                    ("ps_supplycost", DataType::Float64),
                    ("ps_comment", DataType::Utf8),
                ],
            ),
            create_table(
                "customers",
                vec![
                    ("c_custkey", DataType::Int64),
                    ("c_name", DataType::Utf8),
                    ("c_address", DataType::Utf8),
                    ("c_nationkey", DataType::Int64),
                    ("c_phone", DataType::Utf8),
                    ("c_acctbal", DataType::Float64),
                    ("c_mktsegment", DataType::Utf8),
                    ("c_comment", DataType::Utf8),
                ],
            ),
            create_table(
                "orders",
                vec![
                    ("o_orderkey", DataType::Int64),
                    ("o_custkey", DataType::Int64),
                    ("o_orderstatus", DataType::Utf8),
                    ("o_totalprice", DataType::Float64),
                    ("o_orderdate", DataType::Utf8), // TODO: Date
                    ("o_orderpriority", DataType::Utf8),
                    ("o_clerk", DataType::Utf8),
                    ("o_shippriority", DataType::Int32),
                    ("o_comment", DataType::Utf8),
                ],
            ),
            create_table(
                "lineitem",
                vec![
                    ("l_orderkey", DataType::Int64),
                    ("l_partkey", DataType::Int64),
                    ("l_suppkey", DataType::Int64),
                    ("l_linenumber", DataType::Int32),
                    ("l_quantity", DataType::Float64),
                    ("l_extendedprice", DataType::Float64),
                    ("l_discount", DataType::Float64),
                    ("l_tax", DataType::Float64),
                    ("l_returnflag", DataType::Utf8),
                    ("l_linestatus", DataType::Utf8),
                    ("l_shipdate", DataType::Utf8),    // TODO: Date
                    ("l_commitdate", DataType::Utf8),  // TODO: Date
                    ("l_receiptdate", DataType::Utf8), // TODO: Date
                    ("l_shipinstruct", DataType::Utf8),
                    ("l_shipmode", DataType::Utf8),
                    ("l_comment", DataType::Utf8),
                ],
            ),
            create_table(
                "nation",
                vec![
                    ("n_nationkey", DataType::Int64),
                    ("n_name", DataType::Utf8),
                    ("n_regionkey", DataType::Int64),
                    ("n_comment", DataType::Utf8),
                ],
            ),
            create_table(
                "region",
                vec![
                    ("r_regionkey", DataType::Int64),
                    ("r_name", DataType::Utf8),
                    ("r_comment", DataType::Utf8),
                ],
            ),
        ];
        TpchCatalog { tables }
    }
}

impl Catalog for TpchCatalog {
    fn table_schema(&self, tbl: &TableReference) -> Result<TableSchema, CatalogError> {
        let resolved = tbl.clone().resolve_with_defaults("db", "tpch");
        let schema = self
            .tables
            .iter()
            .find(|schema| schema.get_reference() == &resolved)
            .ok_or(CatalogError::MissingTable(tbl.to_string()))?;
        Ok(schema.clone())
    }
}
