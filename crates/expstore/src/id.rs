pub trait Id: std::fmt::Display {
    fn to_path_string(&self) -> String;
}
