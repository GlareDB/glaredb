use super::*;

#[derive(Clone, Debug)]
pub struct CreateTunnel {
    pub name: String,
    pub if_not_exists: bool,
    pub options: TunnelOptions,
}
