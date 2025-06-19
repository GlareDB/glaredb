pub mod protocol;

// TODO: How do we want to handle catalogs that provide data sources? Do we want
// to have "Glue" and "Unity" data sources that are separate from the base
// "Delta" data source?
//
// Current thoughts:
//
// - Base delta data source that registers `read_delta`. Cannot creata "delta"
//   catalog.
// - Secondary data sources for unity and glue. Will not register a `read_delta`
//   function, but may register other functions like `read_unity` etc.
//
// Having separate data source for the actual catalogs means we can keep the
// file format and catalog implementation separate. For example, glue could also
// have iceberg, etc tables in it, and having glue as a separate data source
// means we can dispatch to the appropriate table implementation.
