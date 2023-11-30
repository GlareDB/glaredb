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
    "Compute the absolute value of a number.",
    "abs(-1)",
    abs
}
document! {
    "Compute the inverse cosine (arc cosine) of a number.",
    "acos(0.5)",
    acos
}
document! {
    "Compute the inverse hyperbolic cosine of a number.",
    "acosh(1)",
    acosh
}
document! {
    "Compute the inverse sine (arc sine) of a number.",
    "asin(0.5)",
    asin
}
document! {
    "Compute the inverse hyperbolic sine of a number.",
    "asinh(1)",
    asinh
}
document! {
    "Computes the arctangent of a number.",
    "atan(1)",
    atan
}
document! {
    "Computes the arctangent of y/x given y and x.",
    "atan2(1, 1)",
    atan2
}
document! {
    "Compute the inverse hyperbolic tangent of a number.",
    "atanh(0.5)",
    atanh
}
document! {
    "Compute the cube root of a number.",
    "cbrt(27)",
    cbrt
}
document! {
    "Compute the smallest integer greater than or equal to a number.",
    "ceil(1.1)",
    ceil
}
document! {
    "Compute the cosine of a number.",
    "cos(0)",
    cos
}
document! {
    "Compute the cotangent of a number.",
    "cot(1)",
    cot
}
document! {
    "Compute the hyperbolic cosine of a number.",
    "cosh(0)",
    cosh
}
document! {
    "Converts an angle measured in radians to an approximately equivalent angle measured in degrees.",
    "degrees(1)",
    degrees
}
document! {
    "Compute the base-e exponential of a number.",
    "exp(1)",
    exp
}
document! {
    "Compute the factorial of a number.",
    "factorial(5)",
    factorial
}
document! {
    "Compute the largest integer less than or equal to a number.",
    "floor(1.1)",
    floor
}
document! {
    "Compute the greatest common divisor of two integers.",
    "gcd(10, 15)",
    gcd
}
document! {
    "Returns true if the argument is NaN, false otherwise.",
    "isnan(1)",
    isnan
}
document! {
    "Returns true if the argument is zero, false otherwise.",
    "iszero(0)",
    iszero
}
document! {
    "Compute the least common multiple of two integers.",
    "lcm(10, 15)",
    lcm
}
document! {
    "Compute the natural logarithm of a number.",
    "ln(1)",
    ln
}
document! {
    "Compute the logarithm of a number in a specified base.",
    "log(1)",
    log
}
document! {
    "Compute the base-10 logarithm of a number.",
    "log10(1)",
    log10
}
document! {
    "Compute the base-2 logarithm of a number.",
    "log2(1)",
    log2
}
document! {
    "Returns the first argument if it is not NaN, otherwise returns the second argument.",
    "nanvl(1, 2)",
    nanvl
}
document! {
    "Compute the constant π.",
    "pi()",
    pi
}

document! {
    "Compute the power of a number.",
    "pow(2, 3)",
    pow
}
document! {
    "Converts an angle measured in degrees to an approximately equivalent angle measured in radians.",
    "radians(1)",
    radians
}
document! {
    "Compute a pseudo-random number between 0 and 1.",
    "random()",
    random
}
document! {
    "Round a number to the nearest integer.",
    "round(1.1)",
    round
}
document! {
    "Compute the sign of a number.",
    "signum(1)",
    signum
}
document! {
    "Compute the sine of a number.",
    "sin(0)",
    sin
}
document! {
    "Compute the hyperbolic sine of a number.",
    "sinh(0)",
    sinh
}
document! {
    "Compute the square root of a number.",
    "sqrt(4)",
    sqrt
}
document! {
    "Compute the tangent of a number.",
    "tan(0)",
    tan
}
document! {
    "Compute the hyperbolic tangent of a number.",
    "tanh(0)",
    tanh
}
document! {
    "Truncate a number to the nearest integer. If a second argument is provided, truncate to the specified number of decimal places.",
    "trunc(1.1111, 2)",
    trunc
}
document! {
    "Returns the first non-null argument, or null if all arguments are null.",
    "coalesce(null, 1)",
    coalesce
}
document! {
    "Returns null if the arguments are equal, otherwise returns the first argument.",
    "nullif(1, 1)",
    nullif
}
document! {
    "Compute the ASCII code of the first character of a string.",
    "ascii('a')",
    ascii
}
document! {
    "Compute the number of bits in a string.",
    "bit_length('hello')",
    bit_length
}
document! {
    "Remove the longest string containing only characters from a set of characters from the start and end of a string.",
    "btrim('hello', 'eh')",
    btrim
}
document! {
    "Compute the number of characters in a string.",
    "character_length('hello')",
    character_length
}
document! {
    "Concatenate two strings.",
    "concat('hello', 'world')",
    concat
}
document! {
    "Concatenate two strings with a separator.",
    "concat_ws(',', 'hello', 'world')",
    concat_ws
}
document! {
    "Compute the character with the given ASCII code.",
    "chr(97)",
    chr
}
document! {
    "Capitalize the first letter of each word in a string.",
    "initcap('hello world')",
    initcap
}
document! {
    "Extract a substring from the start of a string with the given length.",
    "left('hello', 2)",
    left
}
document! {
    "Convert a string to lowercase.",
    "lower('HELLO')",
    lower
}
document! {
    "Pad a string to the left to a specified length with a sequence of characters.",
    "lpad('hello', 10, '12')",
    lpad
}
document! {
    "Remove all spaces from the start of a string.",
    "ltrim(' hello ')",
    ltrim
}
document! {
    "Compute the number of bytes in a string.",
    "octet_length('hello')",
    octet_length
}
document! {
    "Repeat a string a specified number of times.",
    "repeat('hello', 2)",
    repeat
}
document! {
    "Replace all occurrences of a substring in a string with a new substring.",
    "replace('hello', 'l', 'r')",
    replace
}
document! {
    "Reverse the characters in a string.",
    "reverse('hello')",
    reverse
}
document! {
    "Extract a substring from the end of a string with the given length.",
    "right('hello', 2)",
    right
}
document! {
    "Pad a string to the right to a specified length with a sequence of characters.",
    "rpad('hello', 10, '12')",
    rpad
}
document! {
    "Remove all spaces from the end of a string.",
    "rtrim(' hello ')",
    rtrim
}
document! {
    "Split a string on a delimiter and return the nth field.",
    "split_part('hello.world', '.', 2)",
    split_part
}
document! {
    "Split a string on a delimiter and return an array of the fields.",
    "string_to_array('hello world', ' ')",
    string_to_array
}
document! {
    "Returns true if the first string starts with the second string, false otherwise.",
    "starts_with('hello world', 'hello')",
    starts_with
}
document! {
    "Find the position of the first occurrence of a substring in a string.",
    "strpos('hello world', 'world')",
    strpos
}
document! {
    "Extract a substring from a string with the given start position and length.",
    "substr('hello', 2, 2)",
    substr
}
document! {
    "TODO",
    "TODO",
    to_hex
}
document! {
    "Replace all occurrences of a set of characters in a string with a new set of characters.",
    "translate('hello', 'el', '12')",
    translate
}
document! {
    "Remove all spaces from the beginning and end of a string.",
    "trim(' hello ')",
    trim
}
document! {
    "Convert a string to uppercase.",
    "upper('hello')",
    upper
}
document! {
    "Generate a random UUID.",
    "uuid()",
    uuid
}
document! {
    "Returns true if the first string matches the second string as a regular expression, false otherwise.",
    "regexp_match('hello world', 'hello')",
    regexp_match
}
document! {
    "Replace all occurrences of a substring in a string with a new substring using a regular expression.",
    "regexp_replace('hello world', 'hello', 'goodbye')",
    regexp_replace
}
document! {
    "Returns the current timestamp.",
    "now()",
    now
}
document! {
    "Returns the current date.",
    "current_date()",
    current_date
}
document! {
    "Returns the current time in UTC",
    "current_time()",
    current_time
}
document! {
    "Returns the date binned to the specified interval.",
    "date_bin('15 minutes', TIMESTAMP '2022-01-01 15:07:00', TIMESTAMP '2020-01-01)",
    date_bin
}
document! {
    "Returns the date truncated to the specified unit.",
    "date_trunc('day', '2020-01-01')",
    date_trunc
}
document! {
    "Returns the specified part of a date.",
    "date_part('year', '2020-01-01')",
    date_part
}
document! {
    "Converts a string to a timestamp (Timestamp<ns, UTC>). Alias for `TIMESTAMP <string>`.",
    "to_timestamp('2020-09-08T12:00:00+00:00')",
    to_timestamp
}
document! {
    "Converts a string to a timestamp with millisecond precision (Timestamp<ms, UTC>)",
    "to_timestamp_millis('2020-09-08T12:00:00+00:00')",
    to_timestamp_millis
}
document! {
    "Converts a string to a timestamp with microsecond precision (Timestamp<µs, UTC>)",
    "to_timestamp_micros('2020-09-08T12:00:00+00:00')",
    to_timestamp_micros
}
document! {
    "Converts a string to a timestamp with second precision (Timestamp<s, UTC>)",
    "to_timestamp_seconds('2020-09-08T12:00:00+00:00')",
    to_timestamp_seconds
}
document! {
    "Converts a unix timestamp (seconds since 1970-01-01 00:00:00 UTC) to a timestamp (Timestamp<s, UTC>).",
    "from_unixtime(1600000000)",
    from_unixtime
}
document! {
    "Compute the digest of a string using the specified algorithm. Valid algorithms are: md5, sha224, sha256, sha384, sha512, blake2s, blake2b, blake3",
    "digest('hello', 'sha256')",
    digest
}
document! {
    "Compute the MD5 digest of a string. Alias for `digest(<string>, 'md5')`.",
    "md5('hello')",
    md5
}
document! {
    "Compute the SHA-224 digest of a string. Alias for `digest(<string>, 'sha224')`.",
    "sha224('hello')",
    sha224
}
document! {
    "Compute the SHA-256 digest of a string. Alias for `digest(<string>, 'sha256')`.",
    "sha256('hello')",
    sha256
}
document! {
    "Compute the SHA-384 digest of a string. Alias for `digest(<string>, 'sha384')`.",
    "sha384('hello')",
    sha384
}
document! {
    "Compute the SHA-512 digest of a string. Alias for `digest(<string>, 'sha512')`.",
    "sha512('hello')",
    sha512
}
document! {
    " Encode a string using the specified encoding. Valid encodings are: hex, base64",
    "encode('hello', 'hex')",
    encode
}
document! {
    "Decode a string using the specified encoding. Valid encodings are: hex, base64",
    "decode('68656c6c6f', 'hex')",
    decode
}
document! {
    "Returns the Arrow type of the argument.",
    "arrow_typeof(1)",
    arrow_typeof
}
document! {
    "Append an element to the end of an array.",
    "array_append([1, 2], 3)",
    array_append
}
document! {
    "Concatenate two arrays.",
    "array_concat([1, 2], [3, 4])",
    array_concat
}
document! {
    "Returns the dimensions of an array.",
    "array_dims([[[1]]])",
    array_dims
}
document! {
    "Check if an array is empty. Returns true if empty",
    "empty([])",
    empty
}
document! {
    "Returns the element of an array at the specified index (using one-based indexing)",
    "array_element([1, 2], 1)",
    array_element
}
document! {
    "Flatten an array of arrays.",
    "flatten([[1, 2], [3, 4]])",
    flatten
}
document! {
    "Returns true if the first array contains all elements of the second array",
    "array_has_all([1, 2], [1, 2, 3])",
    array_has_all
}
document! {
    "Returns true if the first array contains any elements of the second array",
    "array_has_any([1, 2], [1, 2, 3])",
    array_has_any
}
document! {
    "Returns true if the array contains the specified element",
    "array_contains([1, 2], 1)",
    array_contains
}
document! {
    "Returns the length of an array.",
    "array_length([1, 2])",
    array_length
}
document! {
    "Returns the number of dimensions of an array.",
    "array_ndims([ [1, 2], [3, 4] ])",
    array_ndims
}
document! {
    "Remove the last element of an array.",
    "array_pop_back([1, 2])",
    array_pop_back
}
document! {
    "Find the position of the first occurrence of an element in an array.",
    "array_position([1, 2], 2)",
    array_position
}
document! {
    "Find the positions of all occurrences of an element in an array.",
    "array_positions([1, 2, 1], 1)",
    array_positions
}
document! {
    "Prepend an element to the start of an array.",
    "array_prepend([1, 2], 3)",
    array_prepend
}
document! {
    "Repeat an element a specified number of times to create an array.",
    "array_repeat(1, 2)",
    array_repeat
}
document! {
    "Remove the first occurrence of an element from an array.",
    "array_remove([1, 2, 1], 1)",
    array_remove
}
document! {
    "Remove the first n occurrences of an element from an array.",
    "array_remove_n([1, 2, 1], 1, 2)",
    array_remove_n
}
document! {
    "Remove all occurrences of an element from an array.",
    "array_remove_all([1, 2, 1], 1)",
    array_remove_all
}
document! {
    "Replace the first occurrence of an element in an array with a new element.",
    "array_replace([1, 2, 1], 1, 3)",
    array_replace
}
document! {
    "Replace the first n occurrences of an element in an array with a new element.",
    "array_replace_n([1, 2, 1], 1, 3, 2)",
    array_replace_n
}
document! {
    "Replace all occurrences of an element in an array with a new element.",
    "array_replace_all([1, 2, 1], 1, 3)",
    array_replace_all
}
document! {
    "Extract a slice from an array.",
    "array_slice([1, 2, 3, 4], 1, 2)",
    array_slice
}
document! {
    "Convert an array to a string with a separator.",
    "array_to_string([1, 2, 3], ',')",
    array_to_string
}
document! {
    "Returns the number of elements in an array.",
    "cardinality([1, 2, 3])",
    cardinality
}
document! {
    "Create an array from a list of elements.",
    "make_array(1, 2, 3)",
    make_array
}
document! {
    "Create a struct from a list of elements. The field names will always be `cN` where N is the index of the element.",
    "struct(1, 'hello')",
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
