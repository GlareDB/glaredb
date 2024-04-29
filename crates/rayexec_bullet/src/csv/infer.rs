// TODO
//
// Steps:
//
// - Infer column delimiters, number of fields per record
//
// Can probably use the `Decoder` with differently configured csv readers that
// repeatedly called on a small sample until we get a configuration that looks
// reasonable (consistent number of fields across all records in the sample).
//
// - Infer types
//
// Try to parse into candidate types, starting at the second record in the
// sample.
//
// - Header inferrence
//
// Determine if there's a header by trying to parse the first record into the
// inferred types from the previous step. If it differs, assume a header.
