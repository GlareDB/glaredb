use super::debug::ScalarNotImplemented;
use crate::arrays::datatype::DataTypeId;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::RawScalarFunction;

pub const FUNCTION_SET_STRUCT_PACK: ScalarFunctionSet = ScalarFunctionSet {
    name: "struct_pack",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::General,
        description: "Creates a struct from the provided values.",
        arguments: &["values..."],
        example: None,
    }],
    functions: &[RawScalarFunction::new(
        &Signature {
            positional_args: &[],
            variadic_arg: Some(DataTypeId::Any),
            return_type: DataTypeId::Struct,
        },
        &ScalarNotImplemented::new("struct_pack"),
    )],
};

pub const FUNCTION_SET_STRUCT_EXTRACT: ScalarFunctionSet = ScalarFunctionSet {
    name: "struct_extract",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::General,
        description: "Extracts a value from a struct.",
        arguments: &["struct"],
        example: None,
    }],
    functions: &[RawScalarFunction::new(
        &Signature {
            positional_args: &[DataTypeId::Struct],
            variadic_arg: None,
            return_type: DataTypeId::Any,
        },
        &ScalarNotImplemented::new("struct_extract"),
    )],
};
