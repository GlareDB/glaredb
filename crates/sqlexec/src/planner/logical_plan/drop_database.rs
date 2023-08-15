#[derive(Clone, Debug)]
pub struct DropDatabase {
    pub names: Vec<String>,
    pub if_exists: bool,
}
