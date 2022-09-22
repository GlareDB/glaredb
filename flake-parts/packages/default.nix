{
  inputs,
  self,
  lib,
  ...
}: {
  perSystem = {
    config,
    pkgs,
    system,
    ...
  }: let
    rust-stable = self.lib.rust-stable system;
    rust-nightly = self.lib.rust-nightly system;

    otherNativeBuildInputs = self.lib.otherNativeBuildInputs pkgs;
    otherBuildInputs = self.lib.otherBuildInputs pkgs;
  in rec {
    packages = {
      default = packages.cli;
      cli = pkgs.rustPlatform.buildRustPackage rec {
        pname = "glaredb-cli";
        version = "0.1.0";

        buildAndTestSubdir = "crates/glaredb";
        src = self.lib.flake_source;
        cargoLock = {
          lockFile = self.lib.cargo_lock;
          outputHashes = {
            "openraft-0.6.4" = "sha256-JXeEzUYeJdxDTYoJvniRt/WfdCT6FHsfauTI5KyZYzA=";
          };
        };
        buildInputs = [rust-stable] ++ otherBuildInputs;
        nativeBuildInputs = otherNativeBuildInputs;

        LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath buildInputs;
        BINDGEN_EXTRA_CLANG_ARGS = "-isystem ${pkgs.llvmPackages.libclang.lib}/lib/clang/${lib.getVersion pkgs.clang}/include";
        LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
        PROTOC = "${pkgs.protobuf}/bin/protoc";
        PROTOC_INCLUDE = "${pkgs.protobuf}/include";
      };
      server_image = pkgs.dockerTools.buildLayeredImage {
        name = "glaredb";
        contents = [packages.cli];
        created = "now";
        config.Cmd = ["${packages.cli}/bin/glaredb"];
      };
    };
  };
}
