#[derive(Clone, Debug)]
pub struct DropCredentials {
    pub names: Vec<String>,
    pub if_exists: bool,
}
