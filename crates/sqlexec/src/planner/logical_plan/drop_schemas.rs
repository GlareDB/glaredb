use super::*;

#[derive(Clone, Debug)]
pub struct DropSchemas {
    pub names: Vec<OwnedSchemaReference>,
    pub if_exists: bool,
    pub cascade: bool,
}
