---
title: Numeric functions
---

# Numeric function reference

<!-- DOCSGEN_START numeric_functions -->

## `abs`

Compute the absolute value of a number.

**Example**: `abs(-5.7)`

**Output**: `5.7`

## `acos`

Compute the arccosine of value.

**Example**: `acos(1)`

**Output**: `0`

## `acosh`

Compute the inverse hyperbolic cosine of value.

**Example**: `acosh(1)`

**Output**: `0`

## `asin`

Compute the arcsine of value.

**Example**: `asin(0)`

**Output**: `0`

## `asinh`

Compute the inverse hyperbolic sine of value.

**Example**: `asinh(0)`

**Output**: `0`

## `atan`

Compute the arctangent of value.

**Example**: `atan(0)`

**Output**: `0`

## `atan2`

Compute the arctangent of y/x.

**Example**: `atan2(1, 1)`

**Output**: `0.7853981633974483`

## `atanh`

Compute the inverse hyperbolic tangent of value.

**Example**: `atanh(0)`

**Output**: `0`

## `cbrt`

Compute the cube root of value.

**Example**: `cbrt(27)`

**Output**: `3`

## `ceil`

Round number up.

**Example**: `ceil(4.1)`

**Output**: `5`

## `ceiling`

**Alias of `ceil`**

Round number up.

**Example**: `ceil(4.1)`

**Output**: `5`

## `cos`

Compute the cosine of a value.

**Example**: `cos(0)`

**Output**: `1`

## `cosh`

Compute the hyperbolic cosine of value.

**Example**: `cosh(0)`

**Output**: `1`

## `cot`

Compute the cotangent of value.

## `degrees`

Convert radians to degrees.

**Example**: `degrees(3.141592653589793)`

**Output**: `180`

## `exp`

Compute `e ^ val`.

**Example**: `exp(1)`

**Output**: `2.718281828459045`

## `factorial`

Compute the factorial of an integer.

**Example**: `factorial(5)`

**Output**: `120`

## `floor`

Round number down.

**Example**: `floor(4.7)`

**Output**: `4`

## `gcd`

Compute the greatest common divisor of two integers.

**Example**: `gcd(12, 8)`

**Output**: `4`

## `is_nan`

**Alias of `isnan`**

Return if the given float is a NaN.

**Example**: `isnan('NaN'::FLOAT)`

**Output**: `true`

## `isfinite`

Return if the given float is finite.

**Example**: `isfinite(1.0)`

**Output**: `true`

## `isinf`

Return if the given float is infinite (positive or negative infinity).

**Example**: `isinf('Infinity'::FLOAT)`

**Output**: `true`

## `isnan`

Return if the given float is a NaN.

**Example**: `isnan('NaN'::FLOAT)`

**Output**: `true`

## `lcm`

Calculates the least common multiple of two integers.

**Example**: `lcm(12, 18)`

**Output**: `36`

## `ln`

Compute natural log of value.

**Example**: `ln(2.718281828459045)`

**Output**: `1`

## `log`

Compute base-10 log of value.

**Example**: `log(100)`

**Output**: `2`

## `log10`

**Alias of `log`**

Compute base-10 log of value.

**Example**: `log(100)`

**Output**: `2`

## `log2`

Compute base-2 log of value.

**Example**: `log2(8)`

**Output**: `3`

## `negate`

Returns the result of multiplying the input value by -1.

**Example**: `negate(-3.5)`

**Output**: `3.5`

## `pi`

Return the value of pi.

**Example**: `pi()`

**Output**: `3.141592653589793`

## `pow`

**Alias of `power`**

Compute base raised to the power of exponent.

**Example**: `power(2, 3)`

**Output**: `8`

## `power`

Compute base raised to the power of exponent.

**Example**: `power(2, 3)`

**Output**: `8`

## `radians`

Convert degrees to radians.

**Example**: `radians(180)`

**Output**: `3.141592653589793`

## `random`

Return a random float between 0 and 1.

**Example**: `random()`

**Output**: `0.7268028627434533`

## `round`

Round a number to a given scale.

**Example**: `round(3.14159, 2)`

**Output**: `3.14`

## `round`

Round a number to the nearest whole value.

**Example**: `round(3.14159)`

**Output**: `3`

## `shl`

Shifts an integer left by a specified number of bits.

**Example**: `shl(4, 1)`

**Output**: `8`

## `shr`

Shifts an integer right by a specified number of bits.

**Example**: `shr(8, 1)`

**Output**: `4`

## `sign`

Sign of the argument (-1, 0, or +1).

**Example**: `sign(-8.4)`

**Output**: `-1`

## `sin`

Compute the sin of value.

**Example**: `sin(0)`

**Output**: `0`

## `sinh`

Compute the hyperbolic sine of value.

**Example**: `sinh(0)`

**Output**: `0`

## `sqrt`

Compute the square root of value.

**Example**: `sqrt(9)`

**Output**: `3`

## `tan`

Compute the tangent of value.

**Example**: `tan(0)`

**Output**: `0`

## `tanh`

Compute the hyperbolic tangent of value.

**Example**: `tanh(0)`

**Output**: `0`

## `trunc`

Truncate number towards zero.

**Example**: `trunc(-1.9)`

**Output**: `-1`

## `xor`

Performs a bitwise XOR operation on two integers.

**Example**: `xor(5, 3)`

**Output**: `6`


<!-- DOCSGEN_END -->
