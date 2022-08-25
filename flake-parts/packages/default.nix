{
  inputs,
  self,
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
  in rec {
    packages = {
      default = packages.cli;
      cli = pkgs.rustPlatform.buildRustPackage {
        pname = "glaredb-cli";
        version = "0.1.0";

        buildAndTestSubdir = "crates/glaredb";
        src = self.lib.flake_source;
        cargoLock = {
          lockFile = self.lib.cargo_lock;
        };
        buildInputs = [rust-stable];
      };
      server_image = pkgs.dockerTools.buildLayeredImage {
        name = "glaredb-tcp-server";
        tag = "latest";
        contents = [packages.cli];
        config.Cmd = ["${packages.cli}/bin/main" "server"];
      };
    };
  };
}
