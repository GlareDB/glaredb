use std::collections::BTreeSet;

use crate::logical::logical_aggregate::GroupingFunction;

pub fn compute_grouping_value(grouping: &GroupingFunction, grouping_set: &BTreeSet<usize>) -> u64 {
    let mut grouping_val: u64 = 0;

    // Reverse iter to match postgres, the right-most
    // column the GROUPING corresponds to the least
    // significant bit.
    for (idx, group_col) in grouping.group_exprs.iter().rev().enumerate() {
        if !grouping_set.contains(group_col) {
            // "null mask"
            grouping_val |= 1 << idx;
        }
    }

    grouping_val
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grouping_values_no_mask() {
        let grouping_set: BTreeSet<usize> = [0, 1].into_iter().collect();

        // GROUPING(col0, col1) FROM t GROUP BY col0, col1
        let grouping = GroupingFunction {
            group_exprs: vec![0, 1],
        };
        assert_eq!(0, compute_grouping_value(&grouping, &grouping_set));

        // GROUPING(col1) FROM t GROUP BY col0, col1
        let grouping = GroupingFunction {
            group_exprs: vec![1],
        };
        assert_eq!(0, compute_grouping_value(&grouping, &grouping_set));

        // GROUPING(col0) FROM t GROUP BY col0, col1
        let grouping = GroupingFunction {
            group_exprs: vec![0],
        };
        assert_eq!(0, compute_grouping_value(&grouping, &grouping_set));
    }

    #[test]
    fn grouping_values_mask() {
        // GROUPING(col0, col1) FROM t GROUP BY CUBE (col0, col1)
        // GROUPING SET (col0, col1)
        let grouping_set: BTreeSet<usize> = [0, 1].into_iter().collect();
        let grouping = GroupingFunction {
            group_exprs: vec![0, 1],
        };
        assert_eq!(0, compute_grouping_value(&grouping, &grouping_set));

        // GROUPING(col0, col1) FROM t GROUP BY CUBE (col0, col1)
        // GROUPING SET (col0, NULL)
        let grouping_set: BTreeSet<usize> = [0].into_iter().collect();
        let grouping = GroupingFunction {
            group_exprs: vec![0, 1],
        };
        assert_eq!(1, compute_grouping_value(&grouping, &grouping_set));

        // GROUPING(col0, col1) FROM t GROUP BY CUBE (col0, col1)
        // GROUPING SET (NULL, col1)
        let grouping_set: BTreeSet<usize> = [1].into_iter().collect();
        let grouping = GroupingFunction {
            group_exprs: vec![0, 1],
        };
        assert_eq!(2, compute_grouping_value(&grouping, &grouping_set));

        // GROUPING(col0, col1) FROM t GROUP BY CUBE (col0, col1)
        // GROUPING SET (NULL, NULL)
        let grouping_set: BTreeSet<usize> = [].into_iter().collect();
        let grouping = GroupingFunction {
            group_exprs: vec![0, 1],
        };
        assert_eq!(3, compute_grouping_value(&grouping, &grouping_set));
    }
}
