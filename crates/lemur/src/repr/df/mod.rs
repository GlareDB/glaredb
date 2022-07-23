use anyhow::{anyhow, Result};
use bitvec::vec::BitVec;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

use crate::repr::expr::{AggregateExpr, ScalarExpr, ScalarExprVec};
use crate::repr::value::{Row, Value, ValueType, ValueVec};

pub mod groupby;
pub use groupby::*;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    pub types: Vec<ValueType>,
}

impl Schema {
    pub fn empty() -> Schema {
        Schema { types: Vec::new() }
    }

    pub fn project(&self, idxs: &[usize]) -> Result<Schema> {
        let mut out = Vec::with_capacity(idxs.len());
        for &idx in idxs.iter() {
            out.push(self.types.get(idx).cloned().ok_or(anyhow!(
                "attempt to project out of bound for schema: {:?}, idx: {}",
                self.types,
                idx
            ))?);
        }
        Ok(out.into())
    }

    pub fn append_type(&mut self, ty: ValueType) {
        self.types.push(ty)
    }

    pub fn extend_from(&mut self, other: Self) {
        self.types.extend(other.types.into_iter())
    }
}

impl From<Vec<ValueType>> for Schema {
    fn from(v: Vec<ValueType>) -> Self {
        Schema { types: v }
    }
}

/// A column-oriented table of data.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct DataFrame {
    columns: Vec<Arc<ValueVec>>,
}

impl DataFrame {
    pub fn empty() -> Self {
        DataFrame {
            columns: Vec::new(),
        }
    }

    pub fn with_schema_and_capacity(schema: &Schema, cap: usize) -> Result<Self> {
        let columns = schema
            .types
            .iter()
            .map(|ty| ValueVec::with_capacity_for_type(ty, cap))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|v| Arc::new(v))
            .collect();

        Ok(DataFrame { columns })
    }

    /// Create a new dataframe from a list of rows.
    ///
    /// Each row must produce the same schema, and all values in each row must
    /// have a known type.
    pub fn from_rows(rows: impl IntoIterator<Item = Row>) -> Result<Self> {
        let mut iter = rows.into_iter();
        let (lower, _) = iter.size_hint();
        let first = match iter.next() {
            Some(row) => row,
            None => return Ok(Self::empty()),
        };

        let schema: Schema = first
            .values
            .iter()
            .map(|v| v.value_type())
            .collect::<Vec<_>>()
            .into();
        let mut df = Self::with_schema_and_capacity(&schema, lower)?;

        df.push_row(first)?;
        for row in iter {
            df.push_row(row)?;
        }

        Ok(df)
    }

    /// Vertically stack all dataframes in the interator. Each dataframe must
    /// have the same schema.
    pub fn from_dataframes(dfs: impl IntoIterator<Item = DataFrame>) -> Result<Self> {
        let mut iter = dfs.into_iter();
        let mut first = match iter.next() {
            Some(df) => df,
            None => Self::empty(),
        };

        for df in iter {
            first = first.vstack(df)?;
        }

        Ok(first)
    }

    /// Create new dataframe from some columns.
    pub fn from_columns(cols: impl IntoIterator<Item = ValueVec>) -> Result<Self> {
        // TODO: Check lengths.
        Ok(DataFrame {
            columns: cols.into_iter().map(|col| Arc::new(col)).collect(),
        })
    }

    pub fn from_expr_vecs(vecs: impl IntoIterator<Item = ScalarExprVec>) -> Result<Self> {
        // TODO: Check lengths.
        let columns = vecs
            .into_iter()
            .map(|v| match v {
                ScalarExprVec::Ref(v) => v,
                ScalarExprVec::Owned(v) => Arc::new(v),
            })
            .collect();
        Ok(DataFrame { columns })
    }

    /// Get the schema of the dataframe.
    pub fn schema(&self) -> Schema {
        self.columns
            .iter()
            .map(|col| col.value_type())
            .collect::<Vec<_>>()
            .into()
    }

    pub fn get_column_ref(&self, idx: usize) -> Option<&Arc<ValueVec>> {
        self.columns.get(idx)
    }

    /// Project columns using th provided scalar expressions.
    pub fn project_exprs<'a>(
        &self,
        exprs: impl IntoIterator<Item = &'a ScalarExpr>,
    ) -> Result<Self> {
        let cols = exprs
            .into_iter()
            .map(|expr| expr.evaluate(self))
            .collect::<Result<Vec<_>>>()?;
        Self::from_expr_vecs(cols)
    }

    /// Project a subset of columns.
    pub fn project(&self, idxs: impl IntoIterator<Item = usize>) -> Result<Self> {
        let selected = idxs
            .into_iter()
            .map(|idx| self.columns.get(idx).cloned())
            .collect::<Option<Vec<_>>>()
            .ok_or(anyhow!("select index out of bounds"))?;

        Ok(DataFrame { columns: selected })
    }

    /// Filter using the provided mask.
    ///
    /// The mask length must equal the number of rows.
    pub fn filter(&self, mask: &BitVec) -> Result<Self> {
        if self.num_rows() != mask.len() {
            return Err(anyhow!("invalid filter mask"));
        }
        Self::from_columns(self.columns.iter().map(|col| col.filter(mask)))
    }

    /// Filter based on the output of evaluating self against the provided
    /// predicate. Evaulating the predicate should result in a bool vector.
    pub fn filter_expr(&self, pred: &ScalarExpr) -> Result<Self> {
        let evaled = pred.evaluate(self)?;
        let bools = evaled
            .as_ref()
            .downcast_bool_vec()
            .ok_or(anyhow!("cannot downcast expresion result to bool vector"))?;
        // TODO: Turn boolvec into bitvec to avoid doing this.
        let mut mask = BitVec::with_capacity(bools.len());
        // TODO: How to handle nulls?
        for v in bools.iter_values() {
            mask.push(*v);
        }
        self.filter(&mask)
    }

    /// Vertically stack one dataframe onto the other.
    pub fn vstack(self, other: Self) -> Result<Self> {
        // Check types.
        if !self
            .columns
            .iter()
            .zip(other.columns.iter())
            .all(|(a, b)| a.value_type() == b.value_type())
        {
            return Err(anyhow!("column types do not match"));
        }

        let mut top = OwnedDataFrame::from_data_frame(self);
        let bottom = OwnedDataFrame::from_data_frame(other);
        let iter = top.columns.iter_mut().zip(bottom.columns.into_iter());
        for (top, bottom) in iter {
            top.try_append(bottom)
                .map_err(|_| anyhow!("column type mismatch"))?;
        }

        Ok(top.into_data_frame())
    }

    /// Horizontally stack stack dataframes where `self` is on the left, and
    /// `other` is on the right.
    ///
    /// Both dataframes need to have the same number of rows.
    pub fn hstack(mut self, other: &Self) -> Result<Self> {
        if self.num_rows() != other.num_rows() {
            return Err(anyhow!("invalid number of rows for hstack"));
        }
        self.columns
            .extend(other.columns.iter().map(|col| col.clone()));

        Ok(self)
    }

    /// Push a row onto this dataframe.
    pub fn push_row(&mut self, row: Row) -> Result<()> {
        if self.arity() != row.arity() {
            return Err(anyhow!(
                "arity mismatch, df: {}, row: {}",
                self.arity(),
                row.arity()
            ));
        }

        if !self
            .columns
            .iter()
            .zip(row.values.iter())
            .all(|(df_col, row_val)| df_col.value_type() == row_val.value_type())
        {
            return Err(anyhow!("row does not match schema of dataframe"));
        }

        let iter = self.columns.iter_mut().zip(row.values.into_iter());
        for (col, val) in iter {
            (*Arc::make_mut(col)).try_push(val)?;
        }

        Ok(())
    }

    /// Reapeat each row `n` times.
    pub fn repeat_each_row(self, n: usize) -> Self {
        // TODO: Try to reuse the columns in self.
        let columns = self
            .columns
            .iter()
            .map(|col| Arc::new(col.repeat_each_value(n)))
            .collect();
        DataFrame { columns }
    }

    /// Vertically repeat self `n` times.
    pub fn vertical_repeat(self, n: usize) -> Self {
        std::iter::repeat(self.clone())
            .take(n - 1)
            .fold(self, |acc, df| acc.vstack(df).unwrap())
    }

    /// Cross join two dataframes.
    pub fn cross_join(self, other: Self) -> Result<Self> {
        let left_repeat = other.num_rows();
        let right_repeat = self.num_rows();

        let left = self.repeat_each_row(left_repeat);
        let right = other.vertical_repeat(right_repeat);

        Ok(left.hstack(&right)?)
    }

    /// Order by and group by the provided columns.
    pub fn order_by_group_by(self, columns: &[usize]) -> Result<Self> {
        let df = SortedGroupByDataFrame::from_dataframe(self, columns)?;
        Ok(df.into_dataframe())
    }

    pub fn aggregate(self, group_by: &[usize], exprs: &[AggregateExpr]) -> Result<Self> {
        let df = SortedGroupByDataFrame::from_dataframe(self, group_by)?;
        df.accumulate(exprs)
    }

    pub fn arity(&self) -> usize {
        self.columns.len()
    }

    /// Get the number of rows for this dataframe.
    pub fn num_rows(&self) -> usize {
        self.columns
            .get(0)
            .and_then(|col| Some(col.len()))
            .unwrap_or(0)
    }
}

impl From<ScalarExprVec> for DataFrame {
    fn from(v: ScalarExprVec) -> Self {
        match v {
            ScalarExprVec::Ref(v) => DataFrame { columns: vec![v] },
            ScalarExprVec::Owned(v) => DataFrame {
                columns: vec![Arc::new(v)],
            },
        }
    }
}

impl fmt::Debug for DataFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "df: num rows: {}, num_cols: {}",
            self.num_rows(),
            self.columns.len()
        )?;
        let mut iters: Vec<_> = self.columns.iter().map(|col| col.iter_values()).collect();
        for i in 0..self.num_rows() {
            writeln!(f)?;
            write!(f, "{}: ", i)?;
            for iter in iters.iter_mut() {
                let val = iter.next().unwrap(); // Bug if we don't get a value.
                write!(f, "{:?} ", val)?;
            }
        }
        Ok(())
    }
}

/// Utility type with all columns owned by this dataframe.
#[derive(Debug)]
struct OwnedDataFrame {
    columns: Vec<ValueVec>,
}

impl OwnedDataFrame {
    fn from_data_frame(df: DataFrame) -> Self {
        let columns = df
            .columns
            .into_iter()
            .map(|col| Arc::try_unwrap(col).unwrap_or_else(|arc| (*arc).clone()))
            .collect();
        OwnedDataFrame { columns }
    }

    fn into_data_frame(self) -> DataFrame {
        DataFrame {
            columns: self.columns.into_iter().map(|col| Arc::new(col)).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn df(cols: impl IntoIterator<Item = ValueVec>) -> DataFrame {
        DataFrame::from_columns(cols).unwrap()
    }

    #[test]
    fn select() {
        let d = df([
            ValueVec::bools(&[true, false, true]),
            ValueVec::int8s(&[4, 5, 6]),
            ValueVec::int32s(&[7, 8, 9]),
        ]);

        let out = d.project([0, 2]).unwrap();

        let expected = df([
            ValueVec::bools(&[true, false, true]),
            ValueVec::int32s(&[7, 8, 9]),
        ]);

        assert_eq!(expected, out);
    }

    #[test]
    fn cross_join() {
        let left = df([
            ValueVec::int8s(&[1, 2, 3]),
            ValueVec::utf8s(&["one", "two", "three"]),
        ]);
        let right = df([ValueVec::int8s(&[4, 5]), ValueVec::utf8s(&["four", "five"])]);

        let out = left.cross_join(right).unwrap();

        let expected = DataFrame::from_columns(vec![
            ValueVec::int8s(&[1, 1, 2, 2, 3, 3]),
            ValueVec::utf8s(&["one", "one", "two", "two", "three", "three"]),
            ValueVec::int8s(&[4, 5, 4, 5, 4, 5]),
            ValueVec::utf8s(&["four", "five", "four", "five", "four", "five"]),
        ])
        .unwrap();

        assert_eq!(expected, out);
    }
}
