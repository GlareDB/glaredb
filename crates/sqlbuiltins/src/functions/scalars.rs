// we make use of the document! macro to generate the documentation for the builtin functions.
// specifically the `stringify!` macro is used to get the name of the function.
// `Abs` would otherwise be `Abs` instead of `abs`. and so on.
#![allow(non_camel_case_types)]

use crate::{
    builtins::{BuiltinFunction, ConstBuiltinFunction},
    document,
};
use datafusion::logical_expr::BuiltinScalarFunction;
use protogen::metastore::types::catalog::FunctionType;

pub struct ArrowCastFunction {}

impl ConstBuiltinFunction for ArrowCastFunction {
    const NAME: &'static str = "arrow_cast";
    const DESCRIPTION: &'static str = "Casts a value to a specified arrow type.";
    const EXAMPLE: &'static str = "arrow_cast(1, 'Int32')";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
}

document! {
    doc => "Compute the absolute value of a number",
    example => "abs(-1)",
    name => abs
}
document! {
    doc => "Compute the inverse cosine (arc cosine) of a number",
    example => "acos(0.5)",
    name => acos
}
document! {
    doc => "Compute the inverse hyperbolic cosine of a number",
    example => "acosh(1)",
    name => acosh
}
document! {
    doc => "Compute the inverse sine (arc sine) of a number",
    example => "asin(0.5)",
    name => asin
}
document! {
    doc => "Compute the inverse hyperbolic sine of a number",
    example => "asinh(1)",
    name => asinh
}
document! {
    doc => "Computes the arctangent of a number",
    example => "atan(1)",
    name => atan
}
document! {
    doc => "Computes the arctangent of y/x given y and x",
    example => "atan2(1, 1)",
    name => atan2
}
document! {
    doc => "Compute the inverse hyperbolic tangent of a number",
    example => "atanh(0.5)",
    name => atanh
}
document! {
    doc => "Compute the cube root of a number",
    example => "cbrt(27)",
    name => cbrt
}
document! {
    doc => "Compute the smallest integer greater than or equal to a number",
    example => "ceil(1.1)",
    name => ceil
}
document! {
    doc => "Compute the cosine of a number",
    example => "cos(0)",
    name => cos
}
document! {
    doc => "Compute the cotangent of a number",
    example => "cot(1)",
    name => cot
}
document! {
    doc => "Compute the hyperbolic cosine of a number",
    example => "cosh(0)",
    name => cosh
}
document! {
    doc => "Converts an angle measured in radians to an approximately equivalent angle measured in degrees",
    example => "degrees(1)",
    name => degrees
}
document! {
    doc => "Compute the base-e exponential of a number",
    example => "exp(1)",
    name => exp
}
document! {
    doc => "Compute the factorial of a number",
    example => "factorial(5)",
    name => factorial
}
document! {
    doc => "Compute the largest integer less than or equal to a number",
    example => "floor(1.1)",
    name => floor
}
document! {
    doc => "Compute the greatest common divisor of two integers",
    example => "gcd(10, 15)",
    name => gcd
}
document! {
    doc => "Returns true if the argument is NaN, false otherwise",
    example => "isnan(1)",
    name => isnan
}
document! {
    doc => "Returns true if the argument is zero, false otherwise",
    example => "iszero(0)",
    name => iszero
}
document! {
    doc => "Compute the least common multiple of two integers",
    example => "lcm(10, 15)",
    name => lcm
}
document! {
    doc => "Compute the natural logarithm of a number",
    example => "ln(1)",
    name => ln
}
document! {
    doc => "Compute the logarithm of a number in a specified base",
    example => "log(1)",
    name => log
}
document! {
    doc => "Compute the base-10 logarithm of a number",
    example => "log10(1)",
    name => log10
}
document! {
    doc => "Compute the base-2 logarithm of a number",
    example => "log2(1)",
    name => log2
}
document! {
    doc => "Returns the first argument if it is not NaN, otherwise returns the second argument",
    example => "nanvl(1, 2)",
    name => nanvl
}
document! {
    doc => "Compute the constant π",
    example => "pi()",
    name => pi
}

document! {
    doc => "Compute the power of a number",
    example => "pow(2, 3)",
    name => pow
}
document! {
    doc => "Converts an angle measured in degrees to an approximately equivalent angle measured in radians",
    example => "radians(1)",
    name => radians
}
document! {
    doc => "Compute a pseudo-random number between 0 and 1",
    example => "random()",
    name => random
}
document! {
    doc => "Round a number to the nearest integer",
    example => "round(1.1)",
    name => round
}
document! {
    doc => "Compute the sign of a number",
    example => "signum(1)",
    name => signum
}
document! {
    doc => "Compute the sine of a number",
    example => "sin(0)",
    name => sin
}
document! {
    doc => "Compute the hyperbolic sine of a number",
    example => "sinh(0)",
    name => sinh
}
document! {
    doc => "Compute the square root of a number",
    example => "sqrt(4)",
    name => sqrt
}
document! {
    doc => "Compute the tangent of a number",
    example => "tan(0)",
    name => tan
}
document! {
    doc => "Compute the hyperbolic tangent of a number",
    example => "tanh(0)",
    name => tanh
}
document! {
    doc => "Truncate a number to the nearest integer. If a second argument is provided, truncate to the specified number of decimal places",
    example => "trunc(1.1111, 2)",
    name => trunc
}
document! {
    doc => "Returns the first non-null argument, or null if all arguments are null",
    example => "coalesce(null, 1)",
    name => coalesce
}
document! {
    doc => "Returns null if the arguments are equal, otherwise returns the first argument",
    example => "nullif(1, 1)",
    name => nullif
}
document! {
    doc => "Compute the ASCII code of the first character of a string",
    example => "ascii('a')",
    name => ascii
}
document! {
    doc => "Compute the number of bits in a string",
    example => "bit_length('hello')",
    name => bit_length
}
document! {
    doc => "Remove the longest string containing only characters from a set of characters from the start and end of a string",
    example => "btrim('hello', 'eh')",
    name => btrim
}
document! {
    doc => "Compute the number of characters in a string",
    example => "character_length('hello')",
    name => character_length
}
document! {
    doc => "Concatenate two strings",
    example => "concat('hello', 'world')",
    name => concat
}
document! {
    doc => "Concatenate two strings with a separator",
    example => "concat_ws(',', 'hello', 'world')",
    name => concat_ws
}
document! {
    doc => "Compute the character with the given ASCII code",
    example => "chr(97)",
    name => chr
}
document! {
    doc => "Capitalize the first letter of each word in a string",
    example => "initcap('hello world')",
    name => initcap
}
document! {
    doc => "Extract a substring from the start of a string with the given length",
    example => "left('hello', 2)",
    name => left
}
document! {
    doc => "Convert a string to lowercase",
    example => "lower('HELLO')",
    name => lower
}
document! {
    doc => "Pad a string to the left to a specified length with a sequence of characters",
    example => "lpad('hello', 10, '12')",
    name => lpad
}
document! {
    doc => "Remove all spaces from the start of a string",
    example => "ltrim(' hello ')",
    name => ltrim
}
document! {
    doc => "Compute the number of bytes in a string",
    example => "octet_length('hello')",
    name => octet_length
}
document! {
    doc => "Repeat a string a specified number of times",
    example => "repeat('hello', 2)",
    name => repeat
}
document! {
    doc => "Replace all occurrences of a substring in a string with a new substring",
    example => "replace('hello', 'l', 'r')",
    name => replace
}
document! {
    doc => "Reverse the characters in a string",
    example => "reverse('hello')",
    name => reverse
}
document! {
    doc => "Extract a substring from the end of a string with the given length",
    example => "right('hello', 2)",
    name => right
}
document! {
    doc => "Pad a string to the right to a specified length with a sequence of characters",
    example => "rpad('hello', 10, '12')",
    name => rpad
}
document! {
    doc => "Remove all spaces from the end of a string",
    example => "rtrim(' hello ')",
    name => rtrim
}
document! {
    doc => "Split a string on a delimiter and return the nth field",
    example => "split_part('hello.world', '.', 2)",
    name => split_part
}
document! {
    doc => "Split a string on a delimiter and return an array of the fields",
    example => "string_to_array('hello world', ' ')",
    name => string_to_array
}
document! {
    doc => "Returns true if the first string starts with the second string, false otherwise",
    example => "starts_with('hello world', 'hello')",
    name => starts_with
}
document! {
    doc => "Find the position of the first occurrence of a substring in a string",
    example => "strpos('hello world', 'world')",
    name => strpos
}
document! {
    doc => "Extract a substring from a string with the given start position and length",
    example => "substr('hello', 2, 2)",
    name => substr
}
document! {
    doc => "Convert a number or binary value to a hexadecimal string",
    example => "to_hex(99999)",
    name => to_hex
}
document! {
    doc => "Replace all occurrences of a set of characters in a string with a new set of characters",
    example => "translate('hello', 'el', '12')",
    name => translate
}
document! {
    doc => "Remove all spaces from the beginning and end of a string",
    example => "trim(' hello ')",
    name => trim
}
document! {
    doc => "Convert a string to uppercase",
    example => "upper('hello')",
    name => upper
}
document! {
    doc => "Generate a random UUID",
    example => "uuid()",
    name => uuid
}
document! {
    doc => "Returns true if the first string matches the second string as a regular expression, false otherwise",
    example => "regexp_match('hello world', 'hello')",
    name => regexp_match
}
document! {
    doc => "Replace all occurrences of a substring in a string with a new substring using a regular expression",
    example => "regexp_replace('hello world', 'hello', 'goodbye')",
    name => regexp_replace
}
document! {
    doc => "Returns the current timestamp",
    example => "now()",
    name => now
}
document! {
    doc => "Returns the current date",
    example => "current_date()",
    name => current_date
}
document! {
    doc => "Returns the current time in UTC",
    example => "current_time()",
    name => current_time
}
document! {
    doc => "Returns the date binned to the specified interval",
    example => "date_bin('15 minutes', TIMESTAMP '2022-01-01 15:07:00', TIMESTAMP '2020-01-01)",
    name => date_bin
}
document! {
    doc => "Returns the date truncated to the specified unit",
    example => "date_trunc('day', '2020-01-01')",
    name => date_trunc
}
document! {
    doc => "Returns the specified part of a date",
    example => "date_part('year', '2020-01-01')",
    name => date_part
}
document! {
    doc => "Converts a string to a timestamp (Timestamp<ns, UTC>). Alias for `TIMESTAMP <string>`",
    example => "to_timestamp('2020-09-08T12:00:00+00:00')",
    name => to_timestamp
}
document! {
    doc => "Converts a string to a timestamp with millisecond precision (Timestamp<ms, UTC>)",
    example => "to_timestamp_millis('2020-09-08T12:00:00+00:00')",
    name => to_timestamp_millis
}
document! {
    doc => "Converts a string to a timestamp with microsecond precision (Timestamp<µs, UTC>)",
    example => "to_timestamp_micros('2020-09-08T12:00:00+00:00')",
    name => to_timestamp_micros
}
document! {
    doc => "Converts a string to a timestamp with second precision (Timestamp<s, UTC>)",
    example => "to_timestamp_seconds('2020-09-08T12:00:00+00:00')",
    name => to_timestamp_seconds
}
document! {
    doc => "Converts a unix timestamp (seconds since 1970-01-01 00:00:00 UTC) to a timestamp (Timestamp<s, UTC>)",
    example => "from_unixtime(1600000000)",
    name => from_unixtime
}
document! {
    doc => "Compute the digest of a string using the specified algorithm. Valid algorithms are: md5, sha224, sha256, sha384, sha512, blake2s, blake2b, blake3",
    example => "digest('hello', 'sha256')",
    name => digest
}
document! {
    doc => "Compute the MD5 digest of a string. Alias for `digest(<string>, 'md5')`",
    example => "md5('hello')",
    name => md5
}
document! {
    doc => "Compute the SHA-224 digest of a string. Alias for `digest(<string>, 'sha224')`",
    example => "sha224('hello')",
    name => sha224
}
document! {
    doc => "Compute the SHA-256 digest of a string. Alias for `digest(<string>, 'sha256')`",
    example => "sha256('hello')",
    name => sha256
}
document! {
    doc => "Compute the SHA-384 digest of a string. Alias for `digest(<string>, 'sha384')`",
    example => "sha384('hello')",
    name => sha384
}
document! {
    doc => "Compute the SHA-512 digest of a string. Alias for `digest(<string>, 'sha512')`",
    example => "sha512('hello')",
    name => sha512
}
document! {
    doc => "Encode a string using the specified encoding. Valid encodings are: hex, base64",
    example => "encode('hello', 'hex')",
    name => encode
}
document! {
    doc => "Decode a string using the specified encoding. Valid encodings are: hex, base64",
    example => "decode('68656c6c6f', 'hex')",
    name => decode
}
document! {
    doc => "Returns the Arrow type of the argument",
    example => "arrow_typeof(1)",
    name => arrow_typeof
}
document! {
    doc => "Append an element to the end of an array",
    example => "array_append([1, 2], 3)",
    name => array_append
}
document! {
    doc => "Concatenate two arrays",
    example => "array_concat([1, 2], [3, 4])",
    name => array_concat
}
document! {
    doc => "Returns the dimensions of an array",
    example => "array_dims([[[1]]])",
    name => array_dims
}
document! {
    doc => "Returns a boolean indicating whether the array is empty",
    example => "empty([])",
    name => empty
}
document! {
    doc => "Returns the element of an array at the specified index (using one-based indexing)",
    example => "array_element([1, 2], 1)",
    name => array_element
}
document! {
    doc => "Flatten an array of arrays",
    example => "flatten([[1, 2], [3, 4]])",
    name => flatten
}
document! {
    doc => "Returns true if the first array contains all elements of the second array",
    example => "array_has_all([1, 2], [1, 2, 3])",
    name => array_has_all
}
document! {
    doc => "Returns true if the first array contains any elements of the second array",
    example => "array_has_any([1, 2], [1, 2, 3])",
    name => array_has_any
}
document! {
    doc => "Returns true if the array contains the specified element",
    example => "array_contains([1, 2], 1)",
    name => array_contains
}
document! {
    doc => "Returns the length of an array",
    example => "array_length([1, 2])",
    name => array_length
}
document! {
    doc => "Returns the number of dimensions of an array",
    example => "array_ndims([ [1, 2], [3, 4] ])",
    name => array_ndims
}
document! {
    doc => "Remove the last element of an array",
    example => "array_pop_back([1, 2])",
    name => array_pop_back
}
document! {
    doc => "Find the position of the first occurrence of an element in an array",
    example => "array_position([1, 2], 2)",
    name => array_position
}
document! {
    doc => "Find the positions of all occurrences of an element in an array",
    example => "array_positions([1, 2, 1], 1)",
    name => array_positions
}
document! {
    doc => "Prepend an element to the start of an array",
    example => "array_prepend([1, 2], 3)",
    name => array_prepend
}
document! {
    doc => "Repeat an element a specified number of times to create an array",
    example => "array_repeat(1, 2)",
    name => array_repeat
}
document! {
    doc => "Remove the first occurrence of an element from an array",
    example => "array_remove([1, 2, 1], 1)",
    name => array_remove
}
document! {
    doc => "Remove the first n occurrences of an element from an array",
    example => "array_remove_n([1, 2, 1], 1, 2)",
    name => array_remove_n
}
document! {
    doc => "Remove all occurrences of an element from an array",
    example => "array_remove_all([1, 2, 1], 1)",
    name => array_remove_all
}
document! {
    doc => "Replace the first occurrence of an element in an array with a new element",
    example => "array_replace([1, 2, 1], 1, 3)",
    name => array_replace
}
document! {
    doc => "Replace the first n occurrences of an element in an array with a new element",
    example => "array_replace_n([1, 2, 1], 1, 3, 2)",
    name => array_replace_n
}
document! {
    doc => "Replace all occurrences of an element in an array with a new element",
    example => "array_replace_all([1, 2, 1], 1, 3)",
    name => array_replace_all
}
document! {
    doc => "Extract a slice from an array",
    example => "array_slice([1, 2, 3, 4], 1, 2)",
    name => array_slice
}
document! {
    doc => "Convert an array to a string with a separator",
    example => "array_to_string([1, 2, 3], ',')",
    name => array_to_string
}
document! {
    doc => "Returns the number of elements in an array",
    example => "cardinality([1, 2, 3])",
    name => cardinality
}
document! {
    doc => "Create an array from a list of elements",
    example => "make_array(1, 2, 3)",
    name => make_array
}
document! {
    doc => "Create a struct from a list of elements. The field names will always be `cN` where N is the index of the element",
    example => "struct(1, 'hello')",
    "struct" => struct_
}

impl BuiltinFunction for BuiltinScalarFunction {
    fn function_type(&self) -> FunctionType {
        FunctionType::Scalar
    }

    fn name(&self) -> &'static str {
        use BuiltinScalarFunction::*;
        match self {
            Abs => abs::NAME,
            Acos => acos::NAME,
            Acosh => acosh::NAME,
            Asin => asin::NAME,
            Asinh => asinh::NAME,
            Atan => atan::NAME,
            Atanh => atanh::NAME,
            Atan2 => atan2::NAME,
            Cbrt => cbrt::NAME,
            Ceil => ceil::NAME,
            Cos => cos::NAME,
            Cot => cot::NAME,
            Cosh => cosh::NAME,
            Degrees => degrees::NAME,
            Exp => exp::NAME,
            Factorial => factorial::NAME,
            Floor => floor::NAME,
            Gcd => gcd::NAME,
            Isnan => isnan::NAME,
            Iszero => iszero::NAME,
            Lcm => lcm::NAME,
            Ln => ln::NAME,
            Log => log::NAME,
            Log10 => log10::NAME,
            Log2 => log2::NAME,
            Nanvl => nanvl::NAME,
            Pi => pi::NAME,
            Power => pow::NAME,
            Radians => radians::NAME,
            Random => random::NAME,
            Round => round::NAME,
            Signum => signum::NAME,
            Sin => sin::NAME,
            Sinh => sinh::NAME,
            Sqrt => sqrt::NAME,
            Tan => tan::NAME,
            Tanh => tanh::NAME,
            Trunc => trunc::NAME,
            Coalesce => coalesce::NAME,
            NullIf => nullif::NAME,
            Ascii => ascii::NAME,
            BitLength => bit_length::NAME,
            Btrim => btrim::NAME,
            CharacterLength => character_length::NAME,
            Concat => concat::NAME,
            ConcatWithSeparator => concat_ws::NAME,
            Chr => chr::NAME,
            InitCap => initcap::NAME,
            Left => left::NAME,
            Lower => lower::NAME,
            Lpad => lpad::NAME,
            Ltrim => ltrim::NAME,
            OctetLength => octet_length::NAME,
            Repeat => repeat::NAME,
            Replace => replace::NAME,
            Reverse => reverse::NAME,
            Right => right::NAME,
            Rpad => rpad::NAME,
            Rtrim => rtrim::NAME,
            SplitPart => split_part::NAME,
            StringToArray => string_to_array::NAME,
            StartsWith => starts_with::NAME,
            Strpos => strpos::NAME,
            Substr => substr::NAME,
            ToHex => to_hex::NAME,
            Translate => translate::NAME,
            Trim => trim::NAME,
            Upper => upper::NAME,
            Uuid => uuid::NAME,
            RegexpMatch => regexp_match::NAME,
            RegexpReplace => regexp_replace::NAME,
            Now => now::NAME,
            CurrentDate => current_date::NAME,
            CurrentTime => current_time::NAME,
            DateBin => date_bin::NAME,
            DateTrunc => date_trunc::NAME,
            DatePart => date_part::NAME,
            ToTimestamp => to_timestamp::NAME,
            ToTimestampMillis => to_timestamp_millis::NAME,
            ToTimestampMicros => to_timestamp_micros::NAME,
            ToTimestampSeconds => to_timestamp_seconds::NAME,
            FromUnixtime => from_unixtime::NAME,
            Digest => digest::NAME,
            MD5 => md5::NAME,
            SHA224 => sha224::NAME,
            SHA256 => sha256::NAME,
            SHA384 => sha384::NAME,
            SHA512 => sha512::NAME,
            Encode => encode::NAME,
            Decode => decode::NAME,
            ArrowTypeof => arrow_typeof::NAME,
            ArrayAppend => array_append::NAME,
            ArrayConcat => array_concat::NAME,
            ArrayDims => array_dims::NAME,
            ArrayEmpty => empty::NAME,
            ArrayElement => array_element::NAME,
            Flatten => flatten::NAME,
            ArrayHasAll => array_has_all::NAME,
            ArrayHasAny => array_has_any::NAME,
            ArrayHas => array_contains::NAME,
            ArrayLength => array_length::NAME,
            ArrayNdims => array_ndims::NAME,
            ArrayPopBack => array_pop_back::NAME,
            ArrayPosition => array_position::NAME,
            ArrayPositions => array_positions::NAME,
            ArrayPrepend => array_prepend::NAME,
            ArrayRepeat => array_repeat::NAME,
            ArrayRemove => array_remove::NAME,
            ArrayRemoveN => array_remove_n::NAME,
            ArrayRemoveAll => array_remove_all::NAME,
            ArrayReplace => array_replace::NAME,
            ArrayReplaceN => array_replace_n::NAME,
            ArrayReplaceAll => array_replace_all::NAME,
            ArraySlice => array_slice::NAME,
            ArrayToString => array_to_string::NAME,
            Cardinality => cardinality::NAME,
            MakeArray => make_array::NAME,
            Struct => struct_::NAME,
        }
    }
    fn sql_example(&self) -> Option<String> {
        use BuiltinScalarFunction::*;
        Some(
            match self {
                Abs => abs::EXAMPLE,
                Acos => acos::EXAMPLE,
                Acosh => acosh::EXAMPLE,
                Asin => asin::EXAMPLE,
                Asinh => asinh::EXAMPLE,
                Atan => atan::EXAMPLE,
                Atanh => atanh::EXAMPLE,
                Atan2 => atan2::EXAMPLE,
                Cbrt => cbrt::EXAMPLE,
                Ceil => ceil::EXAMPLE,
                Cos => cos::EXAMPLE,
                Cot => cot::EXAMPLE,
                Cosh => cosh::EXAMPLE,
                Degrees => degrees::EXAMPLE,
                Exp => exp::EXAMPLE,
                Factorial => factorial::EXAMPLE,
                Floor => floor::EXAMPLE,
                Gcd => gcd::EXAMPLE,
                Isnan => isnan::EXAMPLE,
                Iszero => iszero::EXAMPLE,
                Lcm => lcm::EXAMPLE,
                Ln => ln::EXAMPLE,
                Log => log::EXAMPLE,
                Log10 => log10::EXAMPLE,
                Log2 => log2::EXAMPLE,
                Nanvl => nanvl::EXAMPLE,
                Pi => pi::EXAMPLE,
                Power => pow::EXAMPLE,
                Radians => radians::EXAMPLE,
                Random => random::EXAMPLE,
                Round => round::EXAMPLE,
                Signum => signum::EXAMPLE,
                Sin => sin::EXAMPLE,
                Sinh => sinh::EXAMPLE,
                Sqrt => sqrt::EXAMPLE,
                Tan => tan::EXAMPLE,
                Tanh => tanh::EXAMPLE,
                Trunc => trunc::EXAMPLE,
                Coalesce => coalesce::EXAMPLE,
                NullIf => nullif::EXAMPLE,
                Ascii => ascii::EXAMPLE,
                BitLength => bit_length::EXAMPLE,
                Btrim => btrim::EXAMPLE,
                CharacterLength => character_length::EXAMPLE,
                Concat => concat::EXAMPLE,
                ConcatWithSeparator => concat_ws::EXAMPLE,
                Chr => chr::EXAMPLE,
                InitCap => initcap::EXAMPLE,
                Left => left::EXAMPLE,
                Lower => lower::EXAMPLE,
                Lpad => lpad::EXAMPLE,
                Ltrim => ltrim::EXAMPLE,
                OctetLength => octet_length::EXAMPLE,
                Repeat => repeat::EXAMPLE,
                Replace => replace::EXAMPLE,
                Reverse => reverse::EXAMPLE,
                Right => right::EXAMPLE,
                Rpad => rpad::EXAMPLE,
                Rtrim => rtrim::EXAMPLE,
                SplitPart => split_part::EXAMPLE,
                StringToArray => string_to_array::EXAMPLE,
                StartsWith => starts_with::EXAMPLE,
                Strpos => strpos::EXAMPLE,
                Substr => substr::EXAMPLE,
                ToHex => to_hex::EXAMPLE,
                Translate => translate::EXAMPLE,
                Trim => trim::EXAMPLE,
                Upper => upper::EXAMPLE,
                Uuid => uuid::EXAMPLE,
                RegexpMatch => regexp_match::EXAMPLE,
                RegexpReplace => regexp_replace::EXAMPLE,
                Now => now::EXAMPLE,
                CurrentDate => current_date::EXAMPLE,
                CurrentTime => current_time::EXAMPLE,
                DateBin => date_bin::EXAMPLE,
                DateTrunc => date_trunc::EXAMPLE,
                DatePart => date_part::EXAMPLE,
                ToTimestamp => to_timestamp::EXAMPLE,
                ToTimestampMillis => to_timestamp_millis::EXAMPLE,
                ToTimestampMicros => to_timestamp_micros::EXAMPLE,
                ToTimestampSeconds => to_timestamp_seconds::EXAMPLE,
                FromUnixtime => from_unixtime::EXAMPLE,
                Digest => digest::EXAMPLE,
                MD5 => md5::EXAMPLE,
                SHA224 => sha224::EXAMPLE,
                SHA256 => sha256::EXAMPLE,
                SHA384 => sha384::EXAMPLE,
                SHA512 => sha512::EXAMPLE,
                Encode => encode::EXAMPLE,
                Decode => decode::EXAMPLE,
                ArrowTypeof => arrow_typeof::EXAMPLE,
                ArrayAppend => array_append::EXAMPLE,
                ArrayConcat => array_concat::EXAMPLE,
                ArrayDims => array_dims::EXAMPLE,
                ArrayEmpty => empty::EXAMPLE,
                ArrayElement => array_element::EXAMPLE,
                Flatten => flatten::EXAMPLE,
                ArrayHasAll => array_has_all::EXAMPLE,
                ArrayHasAny => array_has_any::EXAMPLE,
                ArrayHas => array_contains::EXAMPLE,
                ArrayLength => array_length::EXAMPLE,
                ArrayNdims => array_ndims::EXAMPLE,
                ArrayPopBack => array_pop_back::EXAMPLE,
                ArrayPosition => array_position::EXAMPLE,
                ArrayPositions => array_positions::EXAMPLE,
                ArrayPrepend => array_prepend::EXAMPLE,
                ArrayRepeat => array_repeat::EXAMPLE,
                ArrayRemove => array_remove::EXAMPLE,
                ArrayRemoveN => array_remove_n::EXAMPLE,
                ArrayRemoveAll => array_remove_all::EXAMPLE,
                ArrayReplace => array_replace::EXAMPLE,
                ArrayReplaceN => array_replace_n::EXAMPLE,
                ArrayReplaceAll => array_replace_all::EXAMPLE,
                ArraySlice => array_slice::EXAMPLE,
                ArrayToString => array_to_string::EXAMPLE,
                Cardinality => cardinality::EXAMPLE,
                MakeArray => make_array::EXAMPLE,
                Struct => struct_::EXAMPLE,
            }
            .to_string(),
        )
    }
    fn description(&self) -> Option<String> {
        use BuiltinScalarFunction::*;
        Some(
            match self {
                Abs => abs::DESCRIPTION,
                Acos => acos::DESCRIPTION,
                Acosh => acosh::DESCRIPTION,
                Asin => asin::DESCRIPTION,
                Asinh => asinh::DESCRIPTION,
                Atan => atan::DESCRIPTION,
                Atanh => atanh::DESCRIPTION,
                Atan2 => atan2::DESCRIPTION,
                Cbrt => cbrt::DESCRIPTION,
                Ceil => ceil::DESCRIPTION,
                Cos => cos::DESCRIPTION,
                Cot => cot::DESCRIPTION,
                Cosh => cosh::DESCRIPTION,
                Degrees => degrees::DESCRIPTION,
                Exp => exp::DESCRIPTION,
                Factorial => factorial::DESCRIPTION,
                Floor => floor::DESCRIPTION,
                Gcd => gcd::DESCRIPTION,
                Isnan => isnan::DESCRIPTION,
                Iszero => iszero::DESCRIPTION,
                Lcm => lcm::DESCRIPTION,
                Ln => ln::DESCRIPTION,
                Log => log::DESCRIPTION,
                Log10 => log10::DESCRIPTION,
                Log2 => log2::DESCRIPTION,
                Nanvl => nanvl::DESCRIPTION,
                Pi => pi::DESCRIPTION,
                Power => pow::DESCRIPTION,
                Radians => radians::DESCRIPTION,
                Random => random::DESCRIPTION,
                Round => round::DESCRIPTION,
                Signum => signum::DESCRIPTION,
                Sin => sin::DESCRIPTION,
                Sinh => sinh::DESCRIPTION,
                Sqrt => sqrt::DESCRIPTION,
                Tan => tan::DESCRIPTION,
                Tanh => tanh::DESCRIPTION,
                Trunc => trunc::DESCRIPTION,
                Coalesce => coalesce::DESCRIPTION,
                NullIf => nullif::DESCRIPTION,
                Ascii => ascii::DESCRIPTION,
                BitLength => bit_length::DESCRIPTION,
                Btrim => btrim::DESCRIPTION,
                CharacterLength => character_length::DESCRIPTION,
                Concat => concat::DESCRIPTION,
                ConcatWithSeparator => concat_ws::DESCRIPTION,
                Chr => chr::DESCRIPTION,
                InitCap => initcap::DESCRIPTION,
                Left => left::DESCRIPTION,
                Lower => lower::DESCRIPTION,
                Lpad => lpad::DESCRIPTION,
                Ltrim => ltrim::DESCRIPTION,
                OctetLength => octet_length::DESCRIPTION,
                Repeat => repeat::DESCRIPTION,
                Replace => replace::DESCRIPTION,
                Reverse => reverse::DESCRIPTION,
                Right => right::DESCRIPTION,
                Rpad => rpad::DESCRIPTION,
                Rtrim => rtrim::DESCRIPTION,
                SplitPart => split_part::DESCRIPTION,
                StringToArray => string_to_array::DESCRIPTION,
                StartsWith => starts_with::DESCRIPTION,
                Strpos => strpos::DESCRIPTION,
                Substr => substr::DESCRIPTION,
                ToHex => to_hex::DESCRIPTION,
                Translate => translate::DESCRIPTION,
                Trim => trim::DESCRIPTION,
                Upper => upper::DESCRIPTION,
                Uuid => uuid::DESCRIPTION,
                RegexpMatch => regexp_match::DESCRIPTION,
                RegexpReplace => regexp_replace::DESCRIPTION,
                Now => now::DESCRIPTION,
                CurrentDate => current_date::DESCRIPTION,
                CurrentTime => current_time::DESCRIPTION,
                DateBin => date_bin::DESCRIPTION,
                DateTrunc => date_trunc::DESCRIPTION,
                DatePart => date_part::DESCRIPTION,
                ToTimestamp => to_timestamp::DESCRIPTION,
                ToTimestampMillis => to_timestamp_millis::DESCRIPTION,
                ToTimestampMicros => to_timestamp_micros::DESCRIPTION,
                ToTimestampSeconds => to_timestamp_seconds::DESCRIPTION,
                FromUnixtime => from_unixtime::DESCRIPTION,
                Digest => digest::DESCRIPTION,
                MD5 => md5::DESCRIPTION,
                SHA224 => sha224::DESCRIPTION,
                SHA256 => sha256::DESCRIPTION,
                SHA384 => sha384::DESCRIPTION,
                SHA512 => sha512::DESCRIPTION,
                Encode => encode::DESCRIPTION,
                Decode => decode::DESCRIPTION,
                ArrowTypeof => arrow_typeof::DESCRIPTION,
                ArrayAppend => array_append::DESCRIPTION,
                ArrayConcat => array_concat::DESCRIPTION,
                ArrayDims => array_dims::DESCRIPTION,
                ArrayEmpty => empty::DESCRIPTION,
                ArrayElement => array_element::DESCRIPTION,
                Flatten => flatten::DESCRIPTION,
                ArrayHasAll => array_has_all::DESCRIPTION,
                ArrayHasAny => array_has_any::DESCRIPTION,
                ArrayHas => array_contains::DESCRIPTION,
                ArrayLength => array_length::DESCRIPTION,
                ArrayNdims => array_ndims::DESCRIPTION,
                ArrayPopBack => array_pop_back::DESCRIPTION,
                ArrayPosition => array_position::DESCRIPTION,
                ArrayPositions => array_positions::DESCRIPTION,
                ArrayPrepend => array_prepend::DESCRIPTION,
                ArrayRepeat => array_repeat::DESCRIPTION,
                ArrayRemove => array_remove::DESCRIPTION,
                ArrayRemoveN => array_remove_n::DESCRIPTION,
                ArrayRemoveAll => array_remove_all::DESCRIPTION,
                ArrayReplace => array_replace::DESCRIPTION,
                ArrayReplaceN => array_replace_n::DESCRIPTION,
                ArrayReplaceAll => array_replace_all::DESCRIPTION,
                ArraySlice => array_slice::DESCRIPTION,
                ArrayToString => array_to_string::DESCRIPTION,
                Cardinality => cardinality::DESCRIPTION,
                MakeArray => make_array::DESCRIPTION,
                Struct => struct_::DESCRIPTION,
            }
            .to_string(),
        )
    }
}
