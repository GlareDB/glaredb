{
  inputs,
  self,
  ...
}: let
  inherit (inputs.gitignore.lib) gitignoreSource;
in {
  flake.lib = {
    flake_source = gitignoreSource ../..;
    cargo_lock = ../../Cargo.lock;
    rust-stable = system: inputs.rust-overlay.packages.${system}.rust;
    rust-nightly = system: inputs.rust-overlay.packages.${system}.rust-nightly;
  };
}
