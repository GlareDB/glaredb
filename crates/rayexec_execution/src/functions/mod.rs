pub mod aggregate;
pub mod scalar;

use rayexec_bullet::field::DataType;

#[derive(Debug, Clone, PartialEq)]
pub enum InputTypes {
    /// Exact number of inputs with the given types.
    Exact(&'static [DataType]),

    /// Variadic number of inputs with the same type.
    Variadic(DataType),

    /// Input is not statically determined. Further checks need to be done.
    Dynamic,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReturnType {
    /// Return type is statically known.
    Static(DataType),

    /// Return type depends entirely on the input, and we can't know ahead of
    /// time.
    ///
    /// This is typically used for compound types.
    Dynamic,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Signature {
    pub input: InputTypes,
    pub return_type: ReturnType,
}

impl Signature {
    /// Return if inputs given data types satisfy this signature.
    fn inputs_satisfy_signature(&self, inputs: &[DataType]) -> bool {
        match &self.input {
            InputTypes::Exact(expected) => inputs == *expected,
            InputTypes::Variadic(typ) => inputs.iter().all(|input| input == typ),
            InputTypes::Dynamic => true,
        }
    }
}
