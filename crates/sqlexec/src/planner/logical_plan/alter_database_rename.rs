#[derive(Clone, Debug)]
pub struct AlterDatabaseRename {
    pub name: String,
    pub new_name: String,
}
