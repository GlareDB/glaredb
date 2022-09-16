# GlareDB nix packages

Within this nix file (`default.nix`) a number of packages are defined that may be built using `nix build .#{package_name}`.
The output will be created as `result` in the current directory.

## rust binaries

The utility function `pkgs.rustPlatform.buildRustPackage` will build binaries using cargo (by default). In its most simple form it looks like this:
```nix
cli = pkgs.rustPlatform.buildRustPackage {
  pname = "glaredb-cli";
  version = "0.1.0";

  buildAndTestSubdir = "crates/glaredb";
  src = ./.;
  cargoLock = {
    lockFile = ./Cargo.lock;
  };
};
```
Since there are more dependencies that have configuration options that need to be configured, the actual GlareDB cli has extra packages included and environment variables configured.
Additionally, the files are organized outside the root directory so the paths `./.` and `./Cargo.lock` will need adjusted when used (see current usage).

## container images

Packages may create container images in tarball format.

Nix expression to create an image derivation: 
```nix
server_image = pkgs.dockerTools.buildLayeredImage {
    name = "glaredb";
    contents = [packages.cli];
    created = "now";
    config.Cmd = ["${packages.cli}/bin/glaredb"];
};
```
In this case `packages.cli` is the above cli package that exports a binary.
The binary is included in the container and ran using `config.Cmd`.

Commands to build and import the image into docker: 
```
nix build .#server_image
docker load --input result
```
For options, see [dockerTools in the nix manual](https://nixos.org/manual/nixpkgs/stable/#sec-pkgs-dockerTools)
