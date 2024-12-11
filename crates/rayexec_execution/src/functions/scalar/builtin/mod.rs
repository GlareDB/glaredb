pub mod arith;
pub mod boolean;
pub mod comparison;
pub mod datetime;
pub mod is;
pub mod list;
pub mod negate;
pub mod numeric;
pub mod random;
pub mod similarity;
pub mod string;
pub mod struct_funcs;

use std::sync::LazyLock;

use super::ScalarFunction;

// List of all scalar functions.
pub static BUILTIN_SCALAR_FUNCTIONS: LazyLock<Vec<Box<dyn ScalarFunction>>> = LazyLock::new(|| {
    vec![
        // Arith
        Box::new(arith::Add),
        Box::new(arith::Sub),
        Box::new(arith::Mul),
        Box::new(arith::Div),
        Box::new(arith::Rem),
        // Boolean
        Box::new(boolean::And),
        Box::new(boolean::Or),
        // Comparison
        Box::new(comparison::Eq),
        Box::new(comparison::Neq),
        Box::new(comparison::Lt),
        Box::new(comparison::LtEq),
        Box::new(comparison::Gt),
        Box::new(comparison::GtEq),
        // Numeric
        Box::new(numeric::Ceil::new()),
        Box::new(numeric::Floor::new()),
        Box::new(numeric::Abs::new()),
        Box::new(numeric::Acos::new()),
        Box::new(numeric::Asin::new()),
        Box::new(numeric::Atan::new()),
        Box::new(numeric::Cbrt::new()),
        Box::new(numeric::Cos::new()),
        Box::new(numeric::Exp::new()),
        Box::new(numeric::Ln::new()),
        Box::new(numeric::Log::new()),
        Box::new(numeric::Log2::new()),
        Box::new(numeric::Sin::new()),
        Box::new(numeric::Sqrt::new()),
        Box::new(numeric::Tan::new()),
        Box::new(numeric::Degrees::new()),
        Box::new(numeric::Radians::new()),
        Box::new(numeric::IsNan),
        // String
        Box::new(string::Lower),
        Box::new(string::Upper),
        Box::new(string::Repeat),
        Box::new(string::Substring),
        Box::new(string::StartsWith),
        Box::new(string::EndsWith),
        Box::new(string::Contains),
        Box::new(string::Length),
        Box::new(string::ByteLength),
        Box::new(string::BitLength),
        Box::new(string::Concat),
        Box::new(string::RegexpReplace),
        Box::new(string::Ascii),
        Box::new(string::LeftPad),
        Box::new(string::RightPad),
        Box::new(string::LeftTrim::new()),
        Box::new(string::RightTrim::new()),
        Box::new(string::BTrim::new()),
        Box::new(string::Like),
        // Struct
        Box::new(struct_funcs::StructPack),
        // Unary
        Box::new(negate::Negate),
        Box::new(negate::Not),
        // Random
        Box::new(random::Random),
        // List
        Box::new(list::ListExtract),
        Box::new(list::ListValues),
        // Datetime
        Box::new(datetime::DatePart),
        Box::new(datetime::DateTrunc),
        Box::new(datetime::EpochMs),
        Box::new(datetime::Epoch),
        // Is
        Box::new(is::IsNull),
        Box::new(is::IsNotNull),
        Box::new(is::IsTrue),
        Box::new(is::IsNotTrue),
        Box::new(is::IsFalse),
        Box::new(is::IsNotFalse),
        // Distance
        Box::new(similarity::L2Distance),
    ]
});
