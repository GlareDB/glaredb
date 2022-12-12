use std::process::Command;

fn main() {
    let git_tag = match Command::new("git")
        .args(["describe", "--tags", "--always"])
        .output()
    {
        Ok(output) => String::from_utf8(output.stdout).unwrap(),
        Err(_) => String::from("unknown"),
    };
    println!("cargo:rustc-env=GIT_TAG={}", git_tag);
}
