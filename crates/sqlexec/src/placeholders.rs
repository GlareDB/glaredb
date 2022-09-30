use crate::errors::{internal, Result};
use datafusion::sql::sqlparser::ast::{self, ObjectType};
use postgres_types::Type;
use tracing::log::debug;

/// Replace placeholders in the given SQL statement with the given values.
#[tracing::instrument(level = "debug", skip(params))]
pub(crate) fn bind_placeholders(
    statement: ast::Statement,
    params: &[Option<Vec<u8>>],
    param_types: &[i32],
) -> Result<ast::Statement> {
    // TODO: Replace placeholders with actual values
    debug!("bind placeholders");

    match statement {
        ast::Statement::Insert { or, into, table_name, columns, overwrite, source, partitioned, after_columns, table, on } => {
            let source = bind_query(*source, params, param_types)?;
            Ok(ast::Statement::Insert {
                or,
                into,
                table_name,
                columns,
                overwrite,
                source: Box::new(source),
                partitioned,
                after_columns,
                table,
                on,
            })
        }
        other => Ok(other),
    }
}

fn bind_query(query: ast::Query, params: &[Option<Vec<u8>>], param_types: &[i32]) -> Result<ast::Query> {
    let new_query = ast::Query {
        with: query.with,
        body: Box::new(bind_set_expr(*query.body, params, param_types)?),
        order_by: query.order_by,
        limit: query.limit,
        offset: query.offset,
        fetch: query.fetch,
        lock: query.lock,
    };

    Ok(new_query)
}

fn bind_values(values: ast::Values, params: &[Option<Vec<u8>>], param_types: &[i32]) -> Result<ast::Values> {
    values.0.iter().map(|row| {
        row.iter().map(|expr| {
            bind_expr(expr.clone(), params, param_types)
        }).collect::<Result<Vec<_>>>()
    }).collect::<Result<Vec<_>>>().map(|rows| ast::Values(rows.into()))
}

fn bind_set_expr(expr: ast::SetExpr, params: &[Option<Vec<u8>>], param_types: &[i32]) -> Result<ast::SetExpr> {
    match expr {
        ast::SetExpr::Select(select) => {
            Ok(ast::SetExpr::Select(Box::new(bind_select(*select, params, param_types)?)))
        }
        ast::SetExpr::Values(values) => {
            Ok(ast::SetExpr::Values(bind_values(values, params, param_types)?))
        }
        other => Ok(other),
    }
}

fn bind_select(select: ast::Select, params: &[Option<Vec<u8>>], param_types: &[i32]) -> Result<ast::Select> {
    let new_select = ast::Select {
        distinct: select.distinct,
        top: select.top,
        projection: select.projection,
        into: select.into,
        from: select.from,
        lateral_views: select.lateral_views,
        selection: match select.selection {
            Some(selection) => Some(bind_expr(selection, params, param_types)?),
            None => None,
        },
        group_by: select.group_by,
        cluster_by: select.cluster_by,
        distribute_by: select.distribute_by,
        sort_by: select.sort_by,
        having: select.having,
        qualify: select.qualify,
    };

    Ok(new_select)
}

fn bind_value(value: ast::Value, params: &[Option<Vec<u8>>], param_types: &[i32]) -> Result<ast::Value> {
    match value {
        ast::Value::Placeholder(val) => {
            // val - is either $n or ?n
            let param = val[1..].parse::<usize>().unwrap();
            let ty = Type::from_oid(param_types[param] as u32).unwrap();
            match ty {
                Type::INT8 => {
                    // TODO: properly intake the value. It is not encoded as utf8
                    let val = std::str::from_utf8(params[param].as_ref().unwrap()).unwrap();
                    // TODO: determine what the bool in ast::Value::Number is for
                    // and set it correctly
                    
                    todo!("int8");
                },
                Type::VARCHAR => {
                    let val = std::str::from_utf8(params[param].as_ref().unwrap()).unwrap();
                    Ok(ast::Value::SingleQuotedString(val.to_string()))
                },
                Type::FLOAT8 => {
                    todo!("float8");
                }
                other => unimplemented!("type {:?}", other),
            }
        }
        other => Ok(other),
    }
}

fn bind_expr(expr: ast::Expr, params: &[Option<Vec<u8>>], param_types: &[i32]) -> Result<ast::Expr> {
    let val = match expr {
        ast::Expr::Value(value) => ast::Expr::Value(bind_value(value, params, param_types)?),
        ast::Expr::BinaryOp { left, op, right } => ast::Expr::BinaryOp {
            left: Box::new(bind_expr(*left, params, param_types)?),
            op,
            right: Box::new(bind_expr(*right, params, param_types)?),
        },
        other => other,
    };

    Ok(val)
}
