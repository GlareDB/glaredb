mod setup;
use crate::setup::make_cli;


#[test]
/// Assert that we can still load the old catalog without panicking
fn test_catalog_backwards_compat() {
    let mut cmd = make_cli();
    let pwd = std::env::current_dir().unwrap();
    let root_dir = pwd.parent().unwrap().parent().unwrap();
    let old_catalog = root_dir.join("testdata/catalog_compat/v0");
    cmd.args(["-l", old_catalog.to_str().unwrap()])
        .assert()
        .success();
}


#[test]
/// Make sure that we can read the table options from the old catalog
/// The v0 catalog has a table created from the following SQL:
/// ```sql
/// CREATE EXTERNAL TABLE debug_table
/// FROM debug OPTIONS (
///     table_type 'never_ending'
/// );
/// ```
fn test_catalog_backwards_compat_tbl_options() {
    let mut cmd = make_cli();
    let pwd = std::env::current_dir().unwrap();
    let root_dir = pwd.parent().unwrap().parent().unwrap();
    let old_catalog = root_dir.join("testdata/catalog_compat/v0");

    let query = "SELECT * FROM debug_table LIMIT 1";
    cmd.args(["-l", old_catalog.to_str().unwrap(), "-q", query])
        .assert()
        .success();
}
