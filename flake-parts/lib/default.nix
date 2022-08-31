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

    otherNativeBuildInputs = pkgs: with pkgs; [pkgconfig openssl openssl.dev llvmPackages.bintools llvmPackages.libclang];
    otherBuildInputs = pkgs:
      with pkgs; [
        clang
        openssl
      ];
  };
}
