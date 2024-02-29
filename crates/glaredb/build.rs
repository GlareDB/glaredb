fn main() {
    println!("cargo:include=../extensions/glaredb_distance");
    println!("cargo:rustc-link-lib=dylib=glaredb_distance");
    #[cfg(debug_assertions)]
    println!("cargo:rustc-link-search=native=../glaredb_distance/target/debug");

    #[cfg(not(debug_assertions))]
    println!("cargo:rustc-link-search=native=../glaredb_distance/target/release");

    built::write_built_file().expect("Failed to acquire build-time information");
}
