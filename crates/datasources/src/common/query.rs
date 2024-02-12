use std::fmt::Write;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;

use super::errors::Result;
use super::util::{self, Datasource};

pub fn from_exprs(
    datasource: Datasource,
    table: String,
    projection: SchemaRef,
    filters: &[Expr],
    limit: Option<usize>,
) -> Result<String> {
    // Get the projected columns, joined by a ','. This will be put in the
    // 'SELECT ...' portion of the query.
    let projection_string = if projection.fields().is_empty() {
        "*".to_string()
    } else {
        projection
            .fields
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>()
            .join(",")
    };

    let mut query = format!("SELECT {} FROM {}", projection_string, table);

    let predicate_string = exprs_to_predicate_string(datasource, filters)?;
    if !predicate_string.is_empty() {
        write!(&mut query, " WHERE {predicate_string}")?;
    }

    if let Some(limit) = limit {
        write!(&mut query, " LIMIT {limit}")?;
    }

    Ok(query)
}

/// Convert filtering expressions to a predicate string usable with the
/// generated Postgres query.
fn exprs_to_predicate_string(datasource: Datasource, exprs: &[Expr]) -> Result<String> {
    let mut ss = Vec::new();
    let mut buf = String::new();
    for expr in exprs {
        if try_write_expr(datasource, expr, &mut buf)? {
            ss.push(buf);
            buf = String::new();
        }
    }

    Ok(ss.join(" AND "))
}

/// Try to write the expression to the string, returning true if it was written.
fn try_write_expr(
    datasource: Datasource,
    expr: &Expr,
    buf: &mut String,
) -> Result<bool, DataFusionError> {
    match expr {
        Expr::Column(col) => {
            write!(buf, "{}", col)?;
        }
        Expr::Literal(val) => {
            util::encode_literal_to_text(datasource, buf, val)?;
        }
        Expr::IsNull(expr) => {
            if try_write_expr(datasource, expr, buf)? {
                write!(buf, " IS NULL")?;
            } else {
                return Ok(false);
            }
        }
        Expr::IsNotNull(expr) => {
            if try_write_expr(datasource, expr, buf)? {
                write!(buf, " IS NOT NULL")?;
            } else {
                return Ok(false);
            }
        }
        Expr::IsTrue(expr) => {
            if try_write_expr(datasource, expr, buf)? {
                write!(buf, " IS TRUE")?;
            } else {
                return Ok(false);
            }
        }
        Expr::IsFalse(expr) => {
            if try_write_expr(datasource, expr, buf)? {
                write!(buf, " IS FALSE")?;
            } else {
                return Ok(false);
            }
        }
        Expr::BinaryExpr(binary) => {
            if !try_write_expr(datasource, binary.left.as_ref(), buf)? {
                return Ok(false);
            }
            write!(buf, " {} ", binary.op)?;
            if !try_write_expr(datasource, binary.right.as_ref(), buf)? {
                return Ok(false);
            }
        }
        _ => {
            // Unsupported.
            return Ok(false);
        }
    }

    Ok(true)
}
