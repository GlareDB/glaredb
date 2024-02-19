use insta::{assert_debug_snapshot, assert_display_snapshot};
use rayexec_parser::parser::parse;

#[test]
fn parse_select_statements() {
    assert_debug_snapshot!(parse("select 1; select 2").unwrap(), @r###"
    [
        Query(
            Query {
                with: None,
                body: Select(
                    SelectNode {
                        modifier: None,
                        projections: SelectList(
                            [
                                Expr(
                                    Literal(
                                        Number(
                                            "1",
                                        ),
                                    ),
                                ),
                            ],
                        ),
                        from: TableList(
                            [],
                        ),
                        where_expr: None,
                        group_by: None,
                        having: None,
                        order_by: None,
                        limit: None,
                        offset: None,
                    },
                ),
            },
        ),
        Query(
            Query {
                with: None,
                body: Select(
                    SelectNode {
                        modifier: None,
                        projections: SelectList(
                            [
                                Expr(
                                    Literal(
                                        Number(
                                            "2",
                                        ),
                                    ),
                                ),
                            ],
                        ),
                        from: TableList(
                            [],
                        ),
                        where_expr: None,
                        group_by: None,
                        having: None,
                        order_by: None,
                        limit: None,
                        offset: None,
                    },
                ),
            },
        ),
    ]
    "###);
}
