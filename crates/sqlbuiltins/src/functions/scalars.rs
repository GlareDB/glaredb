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
    example => "abs(-1) = 1",
    description => "Compute the absolute value of a number."
);
make_const!(
    ACOS,
    name => "acos",
    example => "acos(0.5) = 1.0471975511965976",
    description => "Compute the inverse cosine (arc cosine) of a number."
);

make_const!(
    ACOSH,
    name => "acosh",
    example => "acosh(1) = 0",
    description => "Compute the inverse hyperbolic cosine of a number."
);
make_const!(
    ASIN,
    name => "asin",
    example => "asin(0.5) = 0.5235987755982988",
    description => "Compute the inverse sine (arc sine) of a number."
);
make_const!(
    ASINH,
    name => "asinh",
    example => "asinh(1) = 0.881373587019543",
    description => "Compute the inverse hyperbolic sine of a number."
);
make_const!(
    ATAN,
    name => "atan",
    example => "atan(1) = 0.7853981633974483",
    description => "Computes the arctangent of a number."
);
make_const!(
    ATAN2,
    name => "atan2",
    example => "atan2(1, 1) = 0.7853982",
    description => "Computes the arctangent of y/x given y and x."
);
make_const!(
    ATANH,
    name => "atanh",
    example => "atanh(0.5) = 0.5493061443340549",
    description => "Compute the inverse hyperbolic tangent of a number."
);
make_const!(
    CBRT,
    name => "cbrt",
    example => "cbrt(27) = 3.0",
    description => "Compute the cube root of a number."
);
make_const!(
    CEIL,
    name => "ceil",
    example => "ceil(1.1) = 2",
    description => "Compute the smallest integer greater than or equal to a number."
);
make_const!(
    COS,
    name => "cos",
    example => "cos(0) = 1",
    description => "Compute the cosine of a number."
);
make_const!(
    COT,
    name => "cot",
    example => "cot(1) = 0.6420926159343306",
    description => "Compute the cotangent of a number."
);
make_const!(
    COSH,
    name => "cosh",
    example => "cosh(0) = 1",
    description => "Compute the hyperbolic cosine of a number."
);
make_const!(
    DEGREES,
    name => "degrees",
    example => "degrees(1) = 57.29577951308232",
    description => "Converts an angle measured in radians to an approximately equivalent angle measured in degrees."
);
make_const!(
    EXP,
    name => "exp",
    example => "exp(1) = 2.718281828459045",
    description => "Compute the base-e exponential of a number."
);
make_const!(
    FACTORIAL,
    name => "factorial",
    example => "factorial(5) = 120",
    description => "Compute the factorial of a number."
);
make_const!(
    FLOOR,
    name => "floor",
    example => "floor(1.1) = 1",
    description => "Compute the largest integer less than or equal to a number."
);
make_const!(
    GCD,
    name => "gcd",
    example => "gcd(10, 15) = 5",
    description => "Compute the greatest common divisor of two integers."
);
make_const!(
    ISNAN,
    name => "isnan",
    example => "isnan(1) = false",
    description => "Returns true if the argument is NaN, false otherwise."
);
make_const!(
    ISZERO,
    name => "iszero",
    example => "iszero(0) = true",
    description => "Returns true if the argument is zero, false otherwise."
);
make_const!(
    LCM,
    name => "lcm",
    example => "lcm(10, 15) = 30",
    description => "Compute the least common multiple of two integers."
);
make_const!(
    LN,
    name => "ln",
    example => "ln(1) = 0",
    description => "Compute the natural logarithm of a number."
);
make_const!(
    LOG,
    name => "log",
    example => "log(1) = 0",
    description => "Compute the logarithm of a number in a specified base."
);
make_const!(
    LOG10,
    name => "log10",
    example => "log10(1) = 0",
    description => "Compute the base-10 logarithm of a number."
);
make_const!(
    LOG2,
    name => "log2",
    example => "log2(1) = 0",
    description => "Compute the base-2 logarithm of a number."
);
make_const!(
    NANVL,
    name => "nanvl",
    example => "nanvl(1, 2) = 1",
    description => "Returns the first argument if it is not NaN, otherwise returns the second argument."
);
make_const!(
    PI,
    name => "pi",
    example => "pi() = 3.141592653589793",
    description => "Compute the constant π."
);

make_const!(
    POWER,
    name => "pow",
    example => "pow(2, 3) = 8",
    description => "Compute the power of a number."
);
make_const!(
    RADIANS,
    name => "radians",
    example => "radians(1) = 0.017453292519943295",
    description => "Converts an angle measured in degrees to an approximately equivalent angle measured in radians."
);
make_const!(
    RANDOM,
    name => "random",
    example => "random() = 0.532767",
    description => "Compute a random number between 0 and 1."
);
make_const!(
    ROUND,
    name => "round",
    example => "round(1.1) = 1",
    description => "Round a number to the nearest integer."
);
make_const!(
    SIGNUM,
    name => "signum",
    example => "signum(1) = 1",
    description => "Compute the sign of a number."
);
make_const!(
    SIN,
    name => "sin",
    example => "sin(0) = 0",
    description => "Compute the sine of a number."
);
make_const!(
    SINH,
    name => "sinh",
    example => "sinh(0) = 0",
    description => "Compute the hyperbolic sine of a number."
);
make_const!(
    SQRT,
    name => "sqrt",
    example => "sqrt(4) = 2",
    description => "Compute the square root of a number."
);
make_const!(
    TAN,
    name => "tan",
    example => "tan(0) = 0",
    description => "Compute the tangent of a number."
);
make_const!(
    TANH,
    name => "tanh",
    example => "tanh(0) = 0",
    description => "Compute the hyperbolic tangent of a number."
);
make_const!(
    TRUNC,
    name => "trunc",
    example => r#"
        trunc(1.1) = 1
        trunc(1.1111, 2) = 1.11
    "#,
    description => "Truncate a number to the nearest integer. If a second argument is provided, truncate to the specified number of decimal places."
);
make_const!(
    COALESCE,
    name => "coalesce",
    example => "coalesce(null, 1) = 1",
    description => "Returns the first non-null argument, or null if all arguments are null."
);
make_const!(
    NULLIF,
    name => "nullif",
    example => "nullif(1, 1) = null",
    description => "Returns null if the arguments are equal, otherwise returns the first argument."
);
make_const!(
    ASCII,
    name => "ascii",
    example => "ascii('a') = 97",
    description => "Compute the ASCII code of the first character of a string."
);
make_const!(
    BIT_LENGTH,
    name => "bit_length",
    example => "bit_length('hello') = 40",
    description => "Compute the number of bits in a string."
);
make_const!(
    BTRIM,
    name => "btrim",
    example => "btrim('hello', 'eh') = 'llo'",
    description => "Remove the longest string containing only characters from a set of characters from the start and end of a string."
);
make_const!(
    CHARACTER_LENGTH,
    name => "character_length",
    example => "character_length('hello') = 5",
    description => "Compute the number of characters in a string."
);
make_const!(
    CONCAT,
    name => "concat",
    example => "concat('hello', 'world') = 'helloworld'",
    description => "Concatenate two strings."
);
make_const!(
    CONCAT_WITH_SEPARATOR,
    name => "concat_ws",
    example => "concat_ws(',', 'hello', 'world') = 'hello,world'",
    description => "Concatenate two strings with a separator."
);
make_const!(
    CHR,
    name => "chr",
    example => "chr(97) = 'a'",
    description => "Compute the character with the given ASCII code."
);
make_const!(
    INITCAP,
    name => "initcap",
    example => "initcap('hello world') = 'Hello World'",
    description => "Capitalize the first letter of each word in a string."
);
make_const!(
    LEFT,
    name => "left",
    example => "left('hello', 2) = 'he'",
    description => "Extract a substring from the start of a string with the given length."
);
make_const!(
    LOWER,
    name => "lower",
    example => "lower('HELLO') = 'hello'",
    description => "Convert a string to lowercase."
);
make_const!(
    LPAD,
    name => "lpad",
    example => "lpad('hello', 10, '12') = '121212hello'",
    description => "Pad a string to the left to a specified length with a sequence of characters."
);
make_const!(
    LTRIM,
    name => "ltrim",
    example => "ltrim(' hello ') = 'hello '",
    description => "Remove all spaces from the start of a string."
);
make_const!(
    OCTET_LENGTH,
    name => "octet_length",
    example => "octet_length('hello') = 5",
    description => "Compute the number of bytes in a string."
);
make_const!(
    REPEAT,
    name => "repeat",
    example => "repeat('hello', 2) = 'hellohello'",
    description => "Repeat a string a specified number of times."
);
make_const!(
    REPLACE,
    name => "replace",
    example => "replace('hello', 'l', 'r') = 'herro'",
    description => "Replace all occurrences of a substring in a string with a new substring."
);
make_const!(
    REVERSE,
    name => "reverse",
    example => "reverse('hello') = 'olleh'",
    description => "Reverse the characters in a string."
);
make_const!(
    RIGHT,
    name => "right",
    example => "right('hello', 2) = 'lo'",
    description => "Extract a substring from the end of a string with the given length."
);
make_const!(
    RPAD,
    name => "rpad",
    example => "rpad('hello', 10, '12') = 'hello121212'",
    description => "Pad a string to the right to a specified length with a sequence of characters."
);
make_const!(
    RTRIM,
    name => "rtrim",
    example => "rtrim(' hello ') = ' hello'",
    description => "Remove all spaces from the end of a string."
);
make_const!(
    SPLIT_PART,
    name => "split_part",
    example => "split_part('hello.world', '.', 2) = 'world'",
    description => "Split a string on a delimiter and return the nth field."
);
make_const!(
    STRING_TO_ARRAY,
    name => "string_to_array",
    example => "string_to_array('hello world', ' ') = ['hello', 'world']",
    description => "Split a string on a delimiter and return an array of the fields."
);
make_const!(
    STARTS_WITH,
    name => "starts_with",
    example => "starts_with('hello world', 'hello') = true",
    description => "Returns true if the first string starts with the second string, false otherwise."
);
make_const!(
    STRPOS,
    name => "strpos",
    example => "strpos('hello world', 'world') = 7",
    description => "Find the position of the first occurrence of a substring in a string."
);
make_const!(
    SUBSTR,
    name => "substr",
    example => "substr('hello', 2, 2) = 'el'",
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
    example => "translate('hello', 'el', '12') = 'h122o'",
    description => "Replace all occurrences of a set of characters in a string with a new set of characters."
);
make_const!(
    TRIM,
    name => "trim",
    example => "trim(' hello ') = 'hello'",
    description => "Remove all spaces from the start and end of a string."
);
make_const!(
    UPPER,
    name => "upper",
    example => "upper('hello') = 'HELLO'",
    description => "Convert a string to uppercase."
);
make_const!(
    UUID,
    name => "uuid",
    example => "uuid() = 'f81d4fae-7dec-11d0-a765-00a0c91e6bf6'",
    description => "Generate a random UUID."
);
make_const!(
    REGEXP_MATCH,
    name => "regexp_match",
    example => "regexp_match('hello world', 'hello') = true",
    description => "Returns true if the first string matches the second string as a regular expression, false otherwise."
);
make_const!(
    REGEXP_REPLACE,
    name => "regexp_replace",
    example => "regexp_replace('hello world', 'hello', 'goodbye') = 'goodbye world'",
    description => "Replace all occurrences of a substring in a string with a new substring using a regular expression."
);
make_const!(
    NOW,
    name => "now",
    example => "now() = '2020-01-01 00:00:00'",
    description => "Returns the current timestamp."
);
make_const!(
    CURRENT_DATE,
    name => "current_date",
    example => "current_date() = '2020-01-01'",
    description => "Returns the current date."
);
make_const!(
    CURRENT_TIME,
    name => "current_time",
    example => "current_time() = '00:00:00.663116'",
    description => "Returns the current time in UTC"
);

impl BuiltinFunction for BuiltinScalarFunction {
    fn name(&self) -> &str {
        use BuiltinScalarFunction::*;
        match self {
            Abs => ABS.0,
            Acos => ACOS.0,
            Acosh => ACOSH.0,
            Asin => ASIN.0,
            Asinh => ASINH.0,
            Atan => ATAN.0,
            Atanh => ATANH.0,
            Atan2 => ATAN2.0,
            Cbrt => CBRT.0,
            Ceil => CEIL.0,
            Cos => COS.0,
            Cot => COT.0,
            Cosh => COSH.0,
            Degrees => DEGREES.0,
            Exp => EXP.0,
            Factorial => FACTORIAL.0,
            Floor => FLOOR.0,
            Gcd => GCD.0,
            Isnan => ISNAN.0,
            Iszero => ISZERO.0,
            Lcm => LCM.0,
            Ln => LN.0,
            Log => LOG.0,
            Log10 => LOG10.0,
            Log2 => LOG2.0,
            Nanvl => NANVL.0,
            Pi => PI.0,
            Power => POWER.0,
            Radians => RADIANS.0,
            Random => RANDOM.0,
            Round => ROUND.0,
            Signum => SIGNUM.0,
            Sin => SIN.0,
            Sinh => SINH.0,
            Sqrt => SQRT.0,
            Tan => TAN.0,
            Tanh => TANH.0,
            Trunc => TRUNC.0,

            // conditional functions
            Coalesce => COALESCE.0,
            NullIf => NULLIF.0,

            // string functions
            Ascii => ASCII.0,
            BitLength => BIT_LENGTH.0,
            Btrim => BTRIM.0,
            CharacterLength => CHARACTER_LENGTH.0,
            Concat => CONCAT.0,
            ConcatWithSeparator => CONCAT_WITH_SEPARATOR.0,
            Chr => CHR.0,
            InitCap => INITCAP.0,
            Left => LEFT.0,
            Lower => LOWER.0,
            Lpad => LPAD.0,
            Ltrim => LTRIM.0,
            OctetLength => OCTET_LENGTH.0,
            Repeat => REPEAT.0,
            Replace => REPLACE.0,
            Reverse => REVERSE.0,
            Right => RIGHT.0,
            Rpad => RPAD.0,
            Rtrim => RTRIM.0,
            SplitPart => SPLIT_PART.0,
            StringToArray => STRING_TO_ARRAY.0,
            StartsWith => STARTS_WITH.0,
            Strpos => STRPOS.0,
            Substr => SUBSTR.0,
            ToHex => TO_HEX.0,
            Translate => TRANSLATE.0,
            Trim => TRIM.0,
            Upper => UPPER.0,
            Uuid => UUID.0,

            // regex functions
            RegexpMatch => REGEXP_MATCH.0,
            RegexpReplace => REGEXP_REPLACE.0,

            // time/date functions
            Now => NOW.0,
            CurrentDate => CURRENT_DATE.0,
            CurrentTime => CURRENT_TIME.0,
            DateBin => "date_bin",
            DateTrunc => "date_trunc",
            DatePart => "date_part",
            ToTimestamp => "to_timestamp",
            ToTimestampMillis => "to_timestamp_millis",
            ToTimestampMicros => "to_timestamp_micros",
            ToTimestampSeconds => "to_timestamp_seconds",
            FromUnixtime => "from_unixtime",

            // hashing functions
            Digest => "digest",
            MD5 => "md5",
            SHA224 => "sha224",
            SHA256 => "sha256",
            SHA384 => "sha384",
            SHA512 => "sha512",

            // encode/decode
            Encode => "encode",
            Decode => "decode",

            // other functions
            ArrowTypeof => "arrow_typeof",

            // array functions
            ArrayAppend => "array_append",
            ArrayConcat => "array_concat",

            ArrayDims => "array_dims",
            ArrayEmpty => "empty",
            ArrayElement => "array_element",
            Flatten => "flatten",
            ArrayHasAll => "array_has_all",
            ArrayHasAny => "array_has_any",
            ArrayHas => "array_contains",
            ArrayLength => "array_length",
            ArrayNdims => "array_ndims",
            ArrayPopBack => "array_pop_back",
            ArrayPosition => "array_position",
            ArrayPositions => "array_positions",
            ArrayPrepend => "array_prepend",
            ArrayRepeat => "array_repeat",
            ArrayRemove => "array_remove",
            ArrayRemoveN => "array_remove_n",
            ArrayRemoveAll => "array_remove_all",
            ArrayReplace => "array_replace",
            ArrayReplaceN => "array_replace_n",
            ArrayReplaceAll => "array_replace_all",
            ArraySlice => "array_slice",
            ArrayToString => "array_to_string",
            Cardinality => "cardinality",
            MakeArray => "make_array",

            // struct functions
            Struct => "struct",
        }
    }
}
