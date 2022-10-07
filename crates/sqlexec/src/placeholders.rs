use crate::errors::Result;
use datafusion::sql::sqlparser::ast::{self};
use pgrepr::value::{Value, Format};
use pgrepr::types::Type;
use tracing::log::debug;

// TODO: we can avoid usage of Vec here
#[derive(Debug, Clone)]
pub struct BindOpts {
    pub(crate) formats: Vec<Format>,
    pub(crate) values: Vec<Option<Vec<u8>>>,
    pub(crate) types: Vec<Type>,
}

impl BindOpts {
    pub fn new(formats: Vec<i16>, values: Vec<Option<Vec<u8>>>, types: Vec<i32>) -> Result<Self> {
        let formats = formats
            .into_iter()
            .map(|f| match f {
                0 => Format::Text,
                1 => Format::Binary,
                other => unimplemented!("format {}", other),
            })
            .collect();

        let types = types
            .into_iter()
            .map(|t| Type::from_oid(t).unwrap())
            .collect();

        Ok(Self {
            formats,
            values,
            types,
        })
    }

    pub(crate) fn get(&self, idx: usize) -> Result<(Value, Type)> {
        let format = self.formats[idx];
        let ty = self.types[idx];
        let buf = self.values[idx].as_ref().unwrap();

        let value = Value::decode(format, ty, buf)?;
        Ok((value, ty))
    }

}

/// Replace placeholders in the given SQL statement with the given values.
#[tracing::instrument(level = "debug", skip(param_formats, param_data, param_types))]
pub(crate) fn bind_placeholders(
    statement: ast::Statement,
    param_formats: &[i16],
    param_data: &[Option<Vec<u8>>],
    param_types: &[i32],
) -> Result<ast::Statement> {
    // TODO: Replace placeholders with actual values
    debug!("bind placeholders");
    let opts = BindOpts::new(param_formats.to_vec(), param_data.to_vec(), param_types.to_vec())?;

    match statement {
        ast::Statement::Insert { or, into, table_name, columns, overwrite, source, partitioned, after_columns, table, on } => {
            let source = bind_query(&opts, *source)?;
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

fn bind_query(opts: &BindOpts, query: ast::Query) -> Result<ast::Query> {
    Ok(ast::Query {
        body: Box::new(bind_set_expr(opts, *query.body)?),
        ..query
    })
}

fn bind_values(opts: &BindOpts, values: ast::Values) -> Result<ast::Values> {
    values.0.iter().map(|row| {
        row.iter().map(|expr| {
            bind_expr(opts, expr.clone())
        }).collect::<Result<Vec<_>>>()
    }).collect::<Result<Vec<_>>>().map(ast::Values)
}

fn bind_set_expr(opts: &BindOpts, expr: ast::SetExpr) -> Result<ast::SetExpr> {
    match expr {
        ast::SetExpr::Select(select) => {
            Ok(ast::SetExpr::Select(Box::new(bind_select(opts, *select)?)))
        }
        ast::SetExpr::Values(values) => {
            Ok(ast::SetExpr::Values(bind_values(opts, values)?))
        }
        other => Ok(other),
    }
}

fn bind_select(opts: &BindOpts, select: ast::Select) -> Result<ast::Select> {
    Ok(ast::Select {
        selection: match select.selection {
            Some(selection) => Some(bind_expr(opts, selection)?),
            None => None,
        },
        ..select
    })
}

fn bind_value(opts: &BindOpts, value: ast::Value) -> Result<ast::Value> {
    match value {
        ast::Value::Placeholder(val) => {
            // val - is either $n or ?n
            let param = val[1..].parse::<usize>().unwrap();
            let (bound_value, _ty) = opts.get(param)?;

            todo!("bind_value: {:?}", bound_value)
        }
        other => Ok(other),
    }
}

fn bind_expr(opts: &BindOpts, expr: ast::Expr) -> Result<ast::Expr> {
    let val = match expr {
        ast::Expr::Value(value) => ast::Expr::Value(bind_value(opts, value)?),
        ast::Expr::BinaryOp { left, op, right } => ast::Expr::BinaryOp {
            left: Box::new(bind_expr(opts, *left)?),
            op,
            right: Box::new(bind_expr(opts, *right)?),
        },
        other => other,
    };

    Ok(val)
}
