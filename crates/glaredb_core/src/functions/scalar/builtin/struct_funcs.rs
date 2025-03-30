use super::debug::ScalarNotImplemented;
use crate::arrays::datatype::DataTypeId;
use crate::functions::Signature;
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::RawScalarFunction;

pub const FUNCTION_SET_STRUCT_PACK: ScalarFunctionSet = ScalarFunctionSet {
    name: "struct_pack",
    aliases: &[],
    doc: None,
    functions: &[RawScalarFunction::new(
        &Signature {
            positional_args: &[],
            variadic_arg: Some(DataTypeId::Any),
            return_type: DataTypeId::Struct,
            doc: None,
        },
        &ScalarNotImplemented::new("struct_pack"),
    )],
};

pub const FUNCTION_SET_STRUCT_EXTRACT: ScalarFunctionSet = ScalarFunctionSet {
    name: "struct_extract",
    aliases: &[],
    doc: None,
    functions: &[RawScalarFunction::new(
        &Signature {
            positional_args: &[DataTypeId::Struct],
            variadic_arg: None,
            return_type: DataTypeId::Any,
            doc: None,
        },
        &ScalarNotImplemented::new("struct_extract"),
    )],
};
