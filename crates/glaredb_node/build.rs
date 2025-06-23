use std::path::Path;
use std::{env, fs};

fn main() {
    napi_build::setup();

    let version = env!("CARGO_PKG_VERSION");
    let package_json_path = Path::new("package.json");

    if package_json_path.exists() {
        let content = fs::read_to_string(package_json_path).expect("Failed to read package.json");

        let mut json: serde_json::Value =
            serde_json::from_str(&content).expect("Failed to parse package.json");

        json["version"] = serde_json::Value::String(version.to_string());

        let updated_content =
            serde_json::to_string_pretty(&json).expect("Failed to serialize package.json");

        fs::write(package_json_path, updated_content).expect("Failed to write package.json");

        println!("cargo:rerun-if-changed=package.json");
        println!("cargo:rerun-if-changed=Cargo.toml");
        println!("cargo:rerun-if-changed=../../../Cargo.toml");
    }
}
