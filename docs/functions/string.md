---
title: String Functions
---

# String function reference

<!-- DOCSGEN_START string_functions -->

## `ascii`

Get the ascii code of the first character of the argument.

**Example**: `ascii('h')`

**Output**: `104`

## `bit_length`

Get the number of bits in a string or blob.

**Example**: `bit_length('tschüß')`

**Output**: `64`

## `btrim`

Trim matching characters from both sides of the string.

**Example**: `trim('->hello<', '<>-')`

**Output**: `hello`

## `byte_length`

Get the number of bytes in a string or blob.

**Example**: `byte_length('tschüß')`

**Output**: `6`

## `char_length`

Alias of `length`.

Get the number of characters in a string.

**Example**: `length('tschüß')`

**Output**: `6`

## `character_length`

Alias of `length`.

Get the number of characters in a string.

**Example**: `length('tschüß')`

**Output**: `6`

## `concat`

Concatenate many strings into a single string.

**Example**: `concat('cat', 'dog', 'mouse')`

**Output**: `catdogmouse`

## `contains`

Check if string contains a search string.

**Example**: `contains('house', 'ou')`

**Output**: `true`

## `ends_with`

Check if a string ends with a suffix.

**Example**: `ends_with('house', 'se')`

**Output**: `true`

## `initcap`

Convert first letter of each word to uppercase.

**Example**: `initcap('hello world')`

**Output**: `Hello World`

## `instr`

Alias of `strpos`.

Returns the position of a substring within a string. Returns 0 if the substring is not found.

**Example**: `strpos('hello', 'll')`

**Output**: `3`

## `left`

Get the leftmost N characters of a string.

**Example**: `left('alphabet', 3)`

**Output**: `alp`

## `length`

Get the number of characters in a string.

**Example**: `length('tschüß')`

**Output**: `6`

## `like`

Check if a string matches the given pattern.

**Example**: `like('hello, world', '%world')`

**Output**: `true`

## `lower`

Convert the string to lowercase.

**Example**: `lower('ABC')`

**Output**: `abc`

## `lpad`

Left pad a string with spaces until the resulting string contains 'count' characters.

**Example**: `lpad('house', 8)`

**Output**: `   house`

## `ltrim`

Trim matching characters from the left side of the string.

**Example**: `ltrim('->hello<', '<>-')`

**Output**: `hello<`

## `md5`

Compute the MD5 hash of a string, returning the result as a hexadecimal string.

**Example**: `md5('hello')`

**Output**: `5d41402abc4b2a76b9719d911017c592`

## `octet_length`

Alias of `byte_length`.

Get the number of bytes in a string or blob.

**Example**: `byte_length('tschüß')`

**Output**: `6`

## `prefix`

Alias of `starts_with`.

Check if a string starts with a prefix.

**Example**: `starts_with('hello', 'he')`

**Output**: `true`

## `repeat`

Repeat a string some number of times.

**Example**: `repeat('abc', 3)`

**Output**: `abcabcabc`

## `replace`

Replace all occurrences in string of substring from with substring to.

**Example**: `replace('abcdefabcdef', 'cd', 'XX')`

**Output**: `abXXefabXXef`

## `reverse`

Reverse the input string.

**Example**: `reverse('hello')`

**Output**: `olleh`

## `right`

Get the rightmost N characters of a string.

**Example**: `right('alphabet', 3)`

**Output**: `bet`

## `rpad`

Right pad a string with spaces until the resulting string contains 'count' characters.

**Example**: `rpad('house', 8)`

**Output**: `house    `

## `rtrim`

Trim whitespace from the right side of the string.

**Example**: `rtrim('  hello ')`

**Output**: `  hello`

## `split_part`

Splits string at occurrences of delimiter and returns the n'th field (counting from one), or when n is negative, returns the |n|'th-from-last field.

**Example**: `split_part('abc~@~def~@~ghi', '~@~', 2)`

**Output**: `def`

## `starts_with`

Check if a string starts with a prefix.

**Example**: `starts_with('hello', 'he')`

**Output**: `true`

## `strpos`

Returns the position of a substring within a string. Returns 0 if the substring is not found.

**Example**: `strpos('hello', 'll')`

**Output**: `3`

## `substr`

Alias of `substring`.

Get a substring of a string starting at an index until the end of the string. The index is 1-based.

**Example**: `substring('alphabet', 3)`

**Output**: `phabet`

## `substring`

Get a substring of a string starting at an index until the end of the string. The index is 1-based.

**Example**: `substring('alphabet', 3)`

**Output**: `phabet`

## `suffix`

Alias of `ends_with`.

Check if a string ends with a suffix.

**Example**: `ends_with('house', 'se')`

**Output**: `true`

## `translate`

Replace each character in string that matches a character in the from set with the corresponding character in the to set. If from is longer than to, occurrences of the extra characters in from are deleted.

**Example**: `translate('12345', '143', 'ax')`

**Output**: `a2x5`

## `trim`

Alias of `btrim`.

Trim matching characters from both sides of the string.

**Example**: `trim('->hello<', '<>-')`

**Output**: `hello`

## `upper`

Convert the string to uppercase.

**Example**: `upper('ABC')`

**Output**: `ABC`


<!-- DOCSGEN_END -->
