---
title: Regexp Function
---

# Regexp Function Reference

<!-- DOCSGEN_START regexp_functions -->

## `regexp_count`

Count the number of non-overlapping occurrences of a regular expression pattern in a string.

**Example**: `regexp_count('abacad', 'a|b')`

**Output**: `4`

## `regexp_like`

Returns true if the string matches the regular expression pattern.

**Example**: `regexp_like('cat dog house', 'dog')`

**Output**: `true`

## `regexp_replace`

Replace the first regular expression match in a string.

**Example**: `regexp_replace('alphabet', '[ae]', 'DOG')`

**Output**: `DOGlphabet`


<!-- DOCSGEN_END -->
