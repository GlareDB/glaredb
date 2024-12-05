/// All builtin views placed in the 'system.glare_catalog' schema.
pub const BUILTIN_VIEWS: &[BuiltinView] =
    &[SHOW_DATABASES_VIEW, SHOW_SCHEMAS_VIEW, SHOW_TABLES_VIEW];

/// Describes a builtin view.
#[derive(Debug)]
pub struct BuiltinView {
    pub name: &'static str,
    pub view: &'static str,
}

pub const SHOW_DATABASES_VIEW: BuiltinView = BuiltinView {
    name: "show_databases",
    view: "
SELECT database_name
FROM list_databases()
ORDER BY database_name;
",
};

pub const SHOW_SCHEMAS_VIEW: BuiltinView = BuiltinView {
    name: "show_schemas",
    view: "
SELECT schema_name
FROM list_schemas()
ORDER BY schema_name;
",
};

pub const SHOW_TABLES_VIEW: BuiltinView = BuiltinView {
    name: "show_tables",
    view: "
SELECT table_name as name
FROM list_tables()
ORDER BY name;
",
};
