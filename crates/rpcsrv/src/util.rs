/// Key used for the connections map.
// TODO: Possibly per user connections?
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ConnKey {
    pub(crate) ip: String,
    pub(crate) port: String,
}
