use anyhow::{anyhow, Result};
use bitvec::vec::BitVec;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::repr::expr::ExprVec;
use crate::repr::value::{ValueType, ValueVec};

pub mod groupby;
pub use groupby::*;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    pub types: Vec<ValueType>,
}

impl From<Vec<ValueType>> for Schema {
    fn from(v: Vec<ValueType>) -> Self {
        Schema { types: v }
    }
}

/// A column-oriented table of data.
#[derive(Debug, Clone, PartialEq)]
pub struct DataFrame {
    columns: Vec<Arc<ValueVec>>,
}

impl DataFrame {
    pub fn empty() -> Self {
        DataFrame {
            columns: Vec::new(),
        }
    }

    /// Create new dataframe from some columns.
    pub fn from_columns(cols: impl IntoIterator<Item = ValueVec>) -> Result<Self> {
        // TODO: Check lengths.
        Ok(DataFrame {
            columns: cols.into_iter().map(|col| Arc::new(col)).collect(),
        })
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
    pub fn hstack(&self, other: &Self) -> Result<Self> {
        if self.num_rows() != other.num_rows() {
            return Err(anyhow!("invalid number of rows for hstack"));
        }
        let mut columns = Vec::with_capacity(self.columns.len() + other.columns.len());
        columns.extend(self.columns.iter().map(|col| col.clone()));
        columns.extend(other.columns.iter().map(|col| col.clone()));

        Ok(DataFrame { columns })
    }

    /// Reapeat each row `n` times.
    pub fn repeat_each_row(self, n: usize) -> Self {
        // TODO: Try to reuse the columns in self.
        let columns = self
            .columns
            .iter()
            .map(|col| Arc::new(col.from_vec_repeat_each_value(n)))
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

    /// Get the number of rows for this dataframe.
    pub fn num_rows(&self) -> usize {
        self.columns
            .get(0)
            .and_then(|col| Some(col.len()))
            .unwrap_or(0)
    }
}

impl From<ExprVec> for DataFrame {
    fn from(v: ExprVec) -> Self {
        match v {
            ExprVec::Ref(v) => DataFrame { columns: vec![v] },
            ExprVec::Owned(v) => DataFrame {
                columns: vec![Arc::new(v)],
            },
        }
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
