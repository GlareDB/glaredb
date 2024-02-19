use insta::{assert_debug_snapshot, assert_display_snapshot};
use rayexec_execution::sql::planner::SqlPlanner;

#[test]
fn plan_select() {
    assert_debug_snapshot!(SqlPlanner::plan("SELECT * FROM customer WHERE id = 1 LIMIT 5").unwrap(), @r###"
    [
        LogicalPlan {
            nodes: [
                UnboundTable(
                    UnboundTable {
                        reference: ObjectReference(
                            [
                                Ident {
                                    value: "customer",
                                },
                            ],
                        ),
                    },
                ),
                Filter(
                    Filter {
                        predicate: Binary(
                            BinaryExpr {
                                left: UnboundIdent(
                                    Ident {
                                        value: "id",
                                    },
                                ),
                                op: Eq,
                                right: Literal(
                                    Int64(
                                        1,
                                    ),
                                ),
                            },
                        ),
                        input: 0,
                    },
                ),
                Projection(
                    Projection {
                        exprs: [
                            UnboundWildcard(
                                Wildcard(
                                    Wildcard {
                                        exclude_cols: [],
                                        replace_cols: [],
                                    },
                                ),
                            ),
                        ],
                        input: 1,
                    },
                ),
                Limit(
                    Limit {
                        limit: 5,
                        input: 2,
                    },
                ),
            ],
        },
    ]
    "###);
}
