#[derive(Clone, Debug)]
pub struct DropTunnel {
    pub names: Vec<String>,
    pub if_exists: bool,
}
