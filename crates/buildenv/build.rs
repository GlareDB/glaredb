use std::process::Command;

/// Environment variable to use to override the git tag. If unset, we'll shell
/// out `git` to get the tag info.
const GIT_TAG_OVERRIDE: &str = "GIT_TAG_OVERRIDE";

fn main() {
    let git_tag = match std::env::var(GIT_TAG_OVERRIDE) {
        Ok(tag) => tag,
        Err(_) => {
            match Command::new("git")
                .args(["describe", "--tags", "--always"])
                .output()
            {
                Ok(output) => String::from_utf8(output.stdout).unwrap(),
                Err(_) => String::from("unknown"),
            }
        }
    };
    println!("cargo:rustc-env=GIT_TAG={}", git_tag);
}
