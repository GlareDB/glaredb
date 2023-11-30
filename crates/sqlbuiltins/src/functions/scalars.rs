use datafusion::logical_expr::BuiltinScalarFunction;

use crate::builtins::BuiltinFunction;
// mostly using a macro here to preserve the formatting.
// rustfmt will otherwise compact the lines.
macro_rules! make_const {
    (
        $var_name:ident,
        name => $name:expr,
        example => $example:expr,
        description => $description:expr
    ) => {
        const $var_name: (&str, &str, &str) = ($name, $example, $description);
    };
}

make_const!(
    ABS,
    name => "abs",
    example => "abs(-1)",
    description => "Compute the absolute value of a number."
);
make_const!(
    ACOS,
    name => "acos",
    example => "acos(0.5)",
    description => "Compute the inverse cosine (arc cosine) of a number."
);

make_const!(
    ACOSH,
    name => "acosh",
    example => "acosh(1)",
    description => "Compute the inverse hyperbolic cosine of a number."
);
make_const!(
    ASIN,
    name => "asin",
    example => "asin(0.5)",
    description => "Compute the inverse sine (arc sine) of a number."
);
make_const!(
    ASINH,
    name => "asinh",
    example => "asinh(1)",
    description => "Compute the inverse hyperbolic sine of a number."
);
make_const!(
    ATAN,
    name => "atan",
    example => "atan(1)",
    description => "Computes the arctangent of a number."
);
make_const!(
    ATAN2,
    name => "atan2",
    example => "atan2(1, 1)",
    description => "Computes the arctangent of y/x given y and x."
);
make_const!(
    ATANH,
    name => "atanh",
    example => "atanh(0.5)",
    description => "Compute the inverse hyperbolic tangent of a number."
);
make_const!(
    CBRT,
    name => "cbrt",
    example => "cbrt(27)",
    description => "Compute the cube root of a number."
);
make_const!(
    CEIL,
    name => "ceil",
    example => "ceil(1.1)",
    description => "Compute the smallest integer greater than or equal to a number."
);
make_const!(
    COS,
    name => "cos",
    example => "cos(0)",
    description => "Compute the cosine of a number."
);
make_const!(
    COT,
    name => "cot",
    example => "cot(1)",
    description => "Compute the cotangent of a number."
);
make_const!(
    COSH,
    name => "cosh",
    example => "cosh(0)",
    description => "Compute the hyperbolic cosine of a number."
);
make_const!(
    DEGREES,
    name => "degrees",
    example => "degrees(1)",
    description => "Converts an angle measured in radians to an approximately equivalent angle measured in degrees."
);
make_const!(
    EXP,
    name => "exp",
    example => "exp(1)",
    description => "Compute the base-e exponential of a number."
);
make_const!(
    FACTORIAL,
    name => "factorial",
    example => "factorial(5)",
    description => "Compute the factorial of a number."
);
make_const!(
    FLOOR,
    name => "floor",
    example => "floor(1.1)",
    description => "Compute the largest integer less than or equal to a number."
);
make_const!(
    GCD,
    name => "gcd",
    example => "gcd(10, 15)",
    description => "Compute the greatest common divisor of two integers."
);
make_const!(
    ISNAN,
    name => "isnan",
    example => "isnan(1)",
    description => "Returns true if the argument is NaN, false otherwise."
);
make_const!(
    ISZERO,
    name => "iszero",
    example => "iszero(0)",
    description => "Returns true if the argument is zero, false otherwise."
);
make_const!(
    LCM,
    name => "lcm",
    example => "lcm(10, 15)",
    description => "Compute the least common multiple of two integers."
);
make_const!(
    LN,
    name => "ln",
    example => "ln(1)",
    description => "Compute the natural logarithm of a number."
);
make_const!(
    LOG,
    name => "log",
    example => "log(1)",
    description => "Compute the logarithm of a number in a specified base."
);
make_const!(
    LOG10,
    name => "log10",
    example => "log10(1)",
    description => "Compute the base-10 logarithm of a number."
);
make_const!(
    LOG2,
    name => "log2",
    example => "log2(1)",
    description => "Compute the base-2 logarithm of a number."
);
make_const!(
    NANVL,
    name => "nanvl",
    example => "nanvl(1, 2)",
    description => "Returns the first argument if it is not NaN, otherwise returns the second argument."
);
make_const!(
    PI,
    name => "pi",
    example => "pi()",
    description => "Compute the constant π."
);

make_const!(
    POWER,
    name => "pow",
    example => "pow(2, 3)",
    description => "Compute the power of a number."
);
make_const!(
    RADIANS,
    name => "radians",
    example => "radians(1)",
    description => "Converts an angle measured in degrees to an approximately equivalent angle measured in radians."
);
make_const!(
    RANDOM,
    name => "random",
    example => "random()",
    description => "Compute a pseudo-random number between 0 and 1."
);
make_const!(
    ROUND,
    name => "round",
    example => "round(1.1)",
    description => "Round a number to the nearest integer."
);
make_const!(
    SIGNUM,
    name => "signum",
    example => "signum(1)",
    description => "Compute the sign of a number."
);
make_const!(
    SIN,
    name => "sin",
    example => "sin(0)",
    description => "Compute the sine of a number."
);
make_const!(
    SINH,
    name => "sinh",
    example => "sinh(0)",
    description => "Compute the hyperbolic sine of a number."
);
make_const!(
    SQRT,
    name => "sqrt",
    example => "sqrt(4)",
    description => "Compute the square root of a number."
);
make_const!(
    TAN,
    name => "tan",
    example => "tan(0)",
    description => "Compute the tangent of a number."
);
make_const!(
    TANH,
    name => "tanh",
    example => "tanh(0)",
    description => "Compute the hyperbolic tangent of a number."
);
make_const!(
    TRUNC,
    name => "trunc",
    example => "trunc(1.1111, 2)",
    description => "Truncate a number to the nearest integer. If a second argument is provided, truncate to the specified number of decimal places."
);
make_const!(
    COALESCE,
    name => "coalesce",
    example => "coalesce(null, 1)",
    description => "Returns the first non-null argument, or null if all arguments are null."
);
make_const!(
    NULLIF,
    name => "nullif",
    example => "nullif(1, 1)",
    description => "Returns null if the arguments are equal, otherwise returns the first argument."
);
make_const!(
    ASCII,
    name => "ascii",
    example => "ascii('a')",
    description => "Compute the ASCII code of the first character of a string."
);
make_const!(
    BIT_LENGTH,
    name => "bit_length",
    example => "bit_length('hello')",
    description => "Compute the number of bits in a string."
);
make_const!(
    BTRIM,
    name => "btrim",
    example => "btrim('hello', 'eh')",
    description => "Remove the longest string containing only characters from a set of characters from the start and end of a string."
);
make_const!(
    CHARACTER_LENGTH,
    name => "character_length",
    example => "character_length('hello')",
    description => "Compute the number of characters in a string."
);
make_const!(
    CONCAT,
    name => "concat",
    example => "concat('hello', 'world')",
    description => "Concatenate two strings."
);
make_const!(
    CONCAT_WITH_SEPARATOR,
    name => "concat_ws",
    example => "concat_ws(',', 'hello', 'world')",
    description => "Concatenate two strings with a separator."
);
make_const!(
    CHR,
    name => "chr",
    example => "chr(97)",
    description => "Compute the character with the given ASCII code."
);
make_const!(
    INITCAP,
    name => "initcap",
    example => "initcap('hello world')",
    description => "Capitalize the first letter of each word in a string."
);
make_const!(
    LEFT,
    name => "left",
    example => "left('hello', 2)",
    description => "Extract a substring from the start of a string with the given length."
);
make_const!(
    LOWER,
    name => "lower",
    example => "lower('HELLO')",
    description => "Convert a string to lowercase."
);
make_const!(
    LPAD,
    name => "lpad",
    example => "lpad('hello', 10, '12')",
    description => "Pad a string to the left to a specified length with a sequence of characters."
);
make_const!(
    LTRIM,
    name => "ltrim",
    example => "ltrim(' hello ')",
    description => "Remove all spaces from the start of a string."
);
make_const!(
    OCTET_LENGTH,
    name => "octet_length",
    example => "octet_length('hello')",
    description => "Compute the number of bytes in a string."
);
make_const!(
    REPEAT,
    name => "repeat",
    example => "repeat('hello', 2)",
    description => "Repeat a string a specified number of times."
);
make_const!(
    REPLACE,
    name => "replace",
    example => "replace('hello', 'l', 'r')",
    description => "Replace all occurrences of a substring in a string with a new substring."
);
make_const!(
    REVERSE,
    name => "reverse",
    example => "reverse('hello')",
    description => "Reverse the characters in a string."
);
make_const!(
    RIGHT,
    name => "right",
    example => "right('hello', 2)",
    description => "Extract a substring from the end of a string with the given length."
);
make_const!(
    RPAD,
    name => "rpad",
    example => "rpad('hello', 10, '12')",
    description => "Pad a string to the right to a specified length with a sequence of characters."
);
make_const!(
    RTRIM,
    name => "rtrim",
    example => "rtrim(' hello ')",
    description => "Remove all spaces from the end of a string."
);
make_const!(
    SPLIT_PART,
    name => "split_part",
    example => "split_part('hello.world', '.', 2)",
    description => "Split a string on a delimiter and return the nth field."
);
make_const!(
    STRING_TO_ARRAY,
    name => "string_to_array",
    example => "string_to_array('hello world', ' ')",
    description => "Split a string on a delimiter and return an array of the fields."
);
make_const!(
    STARTS_WITH,
    name => "starts_with",
    example => "starts_with('hello world', 'hello')",
    description => "Returns true if the first string starts with the second string, false otherwise."
);
make_const!(
    STRPOS,
    name => "strpos",
    example => "strpos('hello world', 'world')",
    description => "Find the position of the first occurrence of a substring in a string."
);
make_const!(
    SUBSTR,
    name => "substr",
    example => "substr('hello', 2, 2)",
    description => "Extract a substring from a string with the given start position and length."
);
make_const!(
    TO_HEX,
    name => "to_hex",
    example => "TODO",
    description => "TODO"
);
make_const!(
    TRANSLATE,
    name => "translate",
    example => "translate('hello', 'el', '12')",
    description => "Replace all occurrences of a set of characters in a string with a new set of characters."
);
make_const!(
    TRIM,
    name => "trim",
    example => "trim(' hello ')",
    description => "Remove all spaces from the beginning and end of a string."
);
make_const!(
    UPPER,
    name => "upper",
    example => "upper('hello')",
    description => "Convert a string to uppercase."
);
make_const!(
    UUID,
    name => "uuid",
    example => "uuid()",
    description => "Generate a random UUID."
);
make_const!(
    REGEXP_MATCH,
    name => "regexp_match",
    example => "regexp_match('hello world', 'hello')",
    description => "Returns true if the first string matches the second string as a regular expression, false otherwise."
);
make_const!(
    REGEXP_REPLACE,
    name => "regexp_replace",
    example => "regexp_replace('hello world', 'hello', 'goodbye')",
    description => "Replace all occurrences of a substring in a string with a new substring using a regular expression."
);
make_const!(
    NOW,
    name => "now",
    example => "now()",
    description => "Returns the current timestamp."
);
make_const!(
    CURRENT_DATE,
    name => "current_date",
    example => "current_date()",
    description => "Returns the current date."
);
make_const!(
    CURRENT_TIME,
    name => "current_time",
    example => "current_time()",
    description => "Returns the current time in UTC"
);
make_const!(
    DATE_BIN,
    name => "date_bin",
    example => "date_bin('15 minutes', TIMESTAMP '2022-01-01 15:07:00', TIMESTAMP '2020-01-01)",
    description => "Returns the date binned to the specified interval."
);
make_const!(
    DATE_TRUNC,
    name => "date_trunc",
    example => "date_trunc('day', '2020-01-01')",
    description => "Returns the date truncated to the specified unit."
);
make_const!(
    DATE_PART,
    name => "date_part",
    example => "date_part('year', '2020-01-01')",
    description => "Returns the specified part of a date."
);
make_const!(
    TO_TIMESTAMP,
    name => "to_timestamp",
    example => "to_timestamp('2020-09-08T12:00:00+00:00')",
    description => "Converts a string to a timestamp (Timestamp<ns, UTC>). Alias for `TIMESTAMP <string>`."
);
make_const!(
    TO_TIMESTAMP_MILLIS,
    name => "to_timestamp_millis",
    example => "to_timestamp_millis('2020-09-08T12:00:00+00:00')",
    description => "Converts a string to a timestamp with millisecond precision (Timestamp<ms, UTC>)"
);
make_const!(
    TO_TIMESTAMP_MICROS,
    name => "to_timestamp_micros",
    example => "to_timestamp_micros('2020-09-08T12:00:00+00:00')",
    description => "Converts a string to a timestamp with microsecond precision (Timestamp<µs, UTC>)"
);
make_const!(
    TO_TIMESTAMP_SECONDS,
    name => "to_timestamp_seconds",
    example => "to_timestamp_seconds('2020-09-08T12:00:00+00:00')",
    description => "Converts a string to a timestamp with second precision (Timestamp<s, UTC>)"
);
make_const!(
    FROM_UNIXTIME,
    name => "from_unixtime",
    example => "from_unixtime(1600000000)",
    description => "Converts a unix timestamp (seconds since 1970-01-01 00:00:00 UTC) to a timestamp (Timestamp<s, UTC>)."
);
make_const!(
    DIGEST,
    name => "digest",
    example => "digest('hello', 'sha256')",
    description => "Compute the digest of a string using the specified algorithm. Valid algorithms are: md5, sha224, sha256, sha384, sha512, blake2s, blake2b, blake3"
);
make_const!(
    MD5_FUNCTION,
    name => "md5",
    example => "md5('hello')",
    description => "Compute the MD5 digest of a string. Alias for `digest(<string>, 'md5')`."
);
make_const!(
    SHA224_FUNCTION,
    name => "sha224",
    example => "sha224('hello')",
    description => "Compute the SHA-224 digest of a string. Alias for `digest(<string>, 'sha224')`."
);
make_const!(
    SHA256_FUNCTION,
    name => "sha256",
    example => "sha256('hello')",
    description => "Compute the SHA-256 digest of a string. Alias for `digest(<string>, 'sha256')`."
);
make_const!(
    SHA384_FUNCTION,
    name => "sha384",
    example => "sha384('hello')",
    description => "Compute the SHA-384 digest of a string. Alias for `digest(<string>, 'sha384')`."
);
make_const!(
    SHA512_FUNCTION,
    name => "sha512",
    example => "sha512('hello')",
    description => "Compute the SHA-512 digest of a string. Alias for `digest(<string>, 'sha512')`."
);
make_const!(
    ENCODE,
    name => "encode",
    example => "encode('hello', 'hex')",
    description => " Encode a string using the specified encoding. Valid encodings are: hex, base64"
);
make_const!(
    DECODE,
    name => "decode",
    example => "decode('68656c6c6f', 'hex')",
    description => "Decode a string using the specified encoding. Valid encodings are: hex, base64"
);
make_const!(
    ARROW_TYPEOF,
    name => "arrow_typeof",
    example => "arrow_typeof(1)",
    description => "Returns the Arrow type of the argument."
);
make_const!(
    ARRAY_APPEND,
    name => "array_append",
    example => "array_append([1, 2], 3)",
    description => "Append an element to the end of an array."
);
make_const!(
    ARRAY_CONCAT,
    name => "array_concat",
    example => "array_concat([1, 2], [3, 4])",
    description => "Concatenate two arrays."
);
make_const!(
    ARRAY_DIMS,
    name => "array_dims",
    example => "array_dims([[[1]]])",
    description => "Returns the dimensions of an array."
);
make_const!(
    ARRAY_EMPTY,
    name => "empty",
    example => "empty([])",
    description => "Check if an array is empty. Returns true if empty"
);
make_const!(
    ARRAY_ELEMENT,
    name => "array_element",
    example => "array_element([1, 2], 1)",
    description => "Returns the element of an array at the specified index (using one-based indexing)"
);
make_const!(
    FLATTEN,
    name => "flatten",
    example => "flatten([[1, 2], [3, 4]])",
    description => "Flatten an array of arrays."
);
make_const!(
    ARRAY_HAS_ALL,
    name => "array_has_all",
    example => "array_has_all([1, 2], [1, 2, 3])",
    description => "Returns true if the first array contains all elements of the second array"
);
make_const!(
    ARRAY_HAS_ANY,
    name => "array_has_any",
    example => "array_has_any([1, 2], [1, 2, 3])",
    description => "Returns true if the first array contains any elements of the second array"
);
make_const!(
    ARRAY_HAS,
    name => "array_contains",
    example => "array_contains([1, 2], 1)",
    description => "Returns true if the array contains the specified element"
);
make_const!(
    ARRAY_LENGTH,
    name => "array_length",
    example => "array_length([1, 2])",
    description => "Returns the length of an array."
);
make_const!(
    ARRAY_NDIMS,
    name => "array_ndims",
    example => "array_ndims([ [1, 2], [3, 4] ])",
    description => "Returns the number of dimensions of an array."
);
make_const!(
    ARRAY_POP_BACK,
    name => "array_pop_back",
    example => "array_pop_back([1, 2])",
    description => "Remove the last element of an array."
);
make_const!(
    ARRAY_POSITION,
    name => "array_position",
    example => "array_position([1, 2], 2)",
    description => "Find the position of the first occurrence of an element in an array."
);
make_const!(
    ARRAY_POSITIONS,
    name => "array_positions",
    example => "array_positions([1, 2, 1], 1)",
    description => "Find the positions of all occurrences of an element in an array."
);
make_const!(
    ARRAY_PREPEND,
    name => "array_prepend",
    example => "array_prepend([1, 2], 3)",
    description => "Prepend an element to the start of an array."
);
make_const!(
    ARRAY_REPEAT,
    name => "array_repeat",
    example => "array_repeat(1, 2)",
    description => "Repeat an element a specified number of times to create an array."
);
make_const!(
    ARRAY_REMOVE,
    name => "array_remove",
    example => "array_remove([1, 2, 1], 1)",
    description => "Remove the first occurrence of an element from an array."
);
make_const!(
    ARRAY_REMOVE_N,
    name => "array_remove_n",
    example => "array_remove_n([1, 2, 1], 1, 2)",
    description => "Remove the first n occurrences of an element from an array."
);
make_const!(
    ARRAY_REMOVE_ALL,
    name => "array_remove_all",
    example => "array_remove_all([1, 2, 1], 1)",
    description => "Remove all occurrences of an element from an array."
);
make_const!(
    ARRAY_REPLACE,
    name => "array_replace",
    example => "array_replace([1, 2, 1], 1, 3)",
    description => "Replace the first occurrence of an element in an array with a new element."
);
make_const!(
    ARRAY_REPLACE_N,
    name => "array_replace_n",
    example => "array_replace_n([1, 2, 1], 1, 3, 2)",
    description => "Replace the first n occurrences of an element in an array with a new element."
);
make_const!(
    ARRAY_REPLACE_ALL,
    name => "array_replace_all",
    example => "array_replace_all([1, 2, 1], 1, 3)",
    description => "Replace all occurrences of an element in an array with a new element."
);
make_const!(
    ARRAY_SLICE,
    name => "array_slice",
    example => "array_slice([1, 2, 3, 4], 1, 2)",
    description => "Extract a slice from an array."
);
make_const!(
    ARRAY_TO_STRING,
    name => "array_to_string",
    example => "array_to_string([1, 2, 3], ',')",
    description => "Convert an array to a string with a separator."
);
make_const!(
    CARDINALITY,
    name => "cardinality",
    example => "cardinality([1, 2, 3])",
    description => "Returns the number of elements in an array."
);
make_const!(
    MAKE_ARRAY,
    name => "make_array",
    example => "make_array(1, 2, 3)",
    description => "Create an array from a list of elements."
);
make_const!(
    STRUCT,
    name => "struct",
    example => "struct(1, 'hello')",
    description => "Create a struct from a list of elements. The field names will always be `cN` where N is the index of the element."
);

fn func_to_const(func: &BuiltinScalarFunction) -> (&str, &str, &str) {
    use BuiltinScalarFunction::*;
    match func {
        Abs => ABS,
        Acos => ACOS,
        Acosh => ACOSH,
        Asin => ASIN,
        Asinh => ASINH,
        Atan => ATAN,
        Atanh => ATANH,
        Atan2 => ATAN2,
        Cbrt => CBRT,
        Ceil => CEIL,
        Cos => COS,
        Cot => COT,
        Cosh => COSH,
        Degrees => DEGREES,
        Exp => EXP,
        Factorial => FACTORIAL,
        Floor => FLOOR,
        Gcd => GCD,
        Isnan => ISNAN,
        Iszero => ISZERO,
        Lcm => LCM,
        Ln => LN,
        Log => LOG,
        Log10 => LOG10,
        Log2 => LOG2,
        Nanvl => NANVL,
        Pi => PI,
        Power => POWER,
        Radians => RADIANS,
        Random => RANDOM,
        Round => ROUND,
        Signum => SIGNUM,
        Sin => SIN,
        Sinh => SINH,
        Sqrt => SQRT,
        Tan => TAN,
        Tanh => TANH,
        Trunc => TRUNC,

        // conditional functions
        Coalesce => COALESCE,
        NullIf => NULLIF,

        // string functions
        Ascii => ASCII,
        BitLength => BIT_LENGTH,
        Btrim => BTRIM,
        CharacterLength => CHARACTER_LENGTH,
        Concat => CONCAT,
        ConcatWithSeparator => CONCAT_WITH_SEPARATOR,
        Chr => CHR,
        InitCap => INITCAP,
        Left => LEFT,
        Lower => LOWER,
        Lpad => LPAD,
        Ltrim => LTRIM,
        OctetLength => OCTET_LENGTH,
        Repeat => REPEAT,
        Replace => REPLACE,
        Reverse => REVERSE,
        Right => RIGHT,
        Rpad => RPAD,
        Rtrim => RTRIM,
        SplitPart => SPLIT_PART,
        StringToArray => STRING_TO_ARRAY,
        StartsWith => STARTS_WITH,
        Strpos => STRPOS,
        Substr => SUBSTR,
        ToHex => TO_HEX,
        Translate => TRANSLATE,
        Trim => TRIM,
        Upper => UPPER,
        Uuid => UUID,

        // regex functions
        RegexpMatch => REGEXP_MATCH,
        RegexpReplace => REGEXP_REPLACE,

        // time/date functions
        Now => NOW,
        CurrentDate => CURRENT_DATE,
        CurrentTime => CURRENT_TIME,
        DateBin => DATE_BIN,
        DateTrunc => DATE_TRUNC,
        DatePart => DATE_PART,
        ToTimestamp => TO_TIMESTAMP,
        ToTimestampMillis => TO_TIMESTAMP_MILLIS,
        ToTimestampMicros => TO_TIMESTAMP_MICROS,
        ToTimestampSeconds => TO_TIMESTAMP_SECONDS,
        FromUnixtime => FROM_UNIXTIME,
        Digest => DIGEST,
        MD5 => MD5_FUNCTION,
        SHA224 => SHA224_FUNCTION,
        SHA256 => SHA256_FUNCTION,
        SHA384 => SHA384_FUNCTION,
        SHA512 => SHA512_FUNCTION,
        Encode => ENCODE,
        Decode => DECODE,
        ArrowTypeof => ARROW_TYPEOF,
        ArrayAppend => ARRAY_APPEND,
        ArrayConcat => ARRAY_CONCAT,
        ArrayDims => ARRAY_DIMS,
        ArrayEmpty => ARRAY_EMPTY,
        ArrayElement => ARRAY_ELEMENT,
        Flatten => FLATTEN,
        ArrayHasAll => ARRAY_HAS_ALL,
        ArrayHasAny => ARRAY_HAS_ANY,
        ArrayHas => ARRAY_HAS,
        ArrayLength => ARRAY_LENGTH,
        ArrayNdims => ARRAY_NDIMS,
        ArrayPopBack => ARRAY_POP_BACK,
        ArrayPosition => ARRAY_POSITION,
        ArrayPositions => ARRAY_POSITIONS,
        ArrayPrepend => ARRAY_PREPEND,
        ArrayRepeat => ARRAY_REPEAT,
        ArrayRemove => ARRAY_REMOVE,
        ArrayRemoveN => ARRAY_REMOVE_N,
        ArrayRemoveAll => ARRAY_REMOVE_ALL,
        ArrayReplace => ARRAY_REPLACE,
        ArrayReplaceN => ARRAY_REPLACE_N,
        ArrayReplaceAll => ARRAY_REPLACE_ALL,
        ArraySlice => ARRAY_SLICE,
        ArrayToString => ARRAY_TO_STRING,
        Cardinality => CARDINALITY,
        MakeArray => MAKE_ARRAY,
        Struct => STRUCT,
    }
}

impl BuiltinFunction for BuiltinScalarFunction {
    fn name(&self) -> &str {
        func_to_const(self).0
    }
    fn sql_example(&self) -> Option<String> {
        Some(func_to_const(self).1.to_string())
    }
    fn description(&self) -> Option<String> {
        Some(func_to_const(self).2.to_string())
    }
}
