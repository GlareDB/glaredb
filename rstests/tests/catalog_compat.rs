mod setup;
use crate::setup::make_cli;

#[test]
/// Assert that we can still load the old catalog without panicking
fn test_catalog_backwards_compat() {
    let pwd = std::env::current_dir().unwrap();
    let root_dir = pwd.parent().unwrap().parent().unwrap();
    let old_catalog = root_dir.join("testdata/catalog_compat/v0");

    make_cli()
        .args(["-l", old_catalog.to_str().unwrap()])
        .assert()
        .success();
}

#[test]
/// Make sure that we can read the table options from the old catalog
fn test_catalog_backwards_compat_tbl_options() {
    let pwd = std::env::current_dir().unwrap();
    let root_dir = pwd.parent().unwrap().parent().unwrap();
    let old_catalog = root_dir.join("testdata/catalog_compat/v0");

    make_cli()
        .args([
            "-l",
            old_catalog.to_str().unwrap(),
            "-q",
            "SELECT * FROM debug_table LIMIT 1",
        ])
        .assert()
        .success();
}
