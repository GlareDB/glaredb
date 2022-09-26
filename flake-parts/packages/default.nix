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
    inputs',
    ...
  }: let
    otherNativeBuildInputs = self.lib.otherNativeBuildInputs pkgs;
    otherBuildInputs = self.lib.otherBuildInputs pkgs;

    craneLib = inputs.crane.lib.${system}.overrideToolchain
      inputs'.fenix.packages.stable.toolchain;

    common-build-args = rec {
      src = lib.cleanSourceWith {
        src = ../..;
        filter = self.lib.filterSrc craneLib;
      };

      buildInputs = otherBuildInputs;
      nativeBuildInputs = otherNativeBuildInputs;
      LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath buildInputs;
      BINDGEN_EXTRA_CLANG_ARGS = "-isystem ${pkgs.llvmPackages.libclang.lib}/lib/clang/${lib.getVersion pkgs.clang}/include";
      LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
      PROTOC = "${pkgs.protobuf}/bin/protoc";
      PROTOC_INCLUDE = "${pkgs.protobuf}/include";
    };
    cargoArtifacts = craneLib.buildDepsOnly ({
      pname = "glaredb";
    } // common-build-args);

    clippy-check = craneLib.cargoClippy ({
      inherit cargoArtifacts;
      cargoClippyExtraArgs = "--all-features -- --deny warnings";
    } // common-build-args);

  in rec {
    checks = {
      inherit clippy-check;
      build-crate = packages.default;
      slt-check = pkgs.stdenv.mkDerivation {
        name = "slt-check";
        buildCommand = ''
          ls ${../..}/testdata/sqllogictests
          ${packages.slt_runner}/bin/slt_runner -- ${../..}/testdata/sqllogictests/*.slt
          echo "slt tests passed"
          exit 1
          touch $out
        '';
      };
    };
    packages = {
      default = packages.cli;

      cli = craneLib.buildPackage ({
        pname = "glaredb-cli";
        inherit cargoArtifacts;
        cargoExtraArgs = "--bin glaredb";
      } // common-build-args);

      server_image = pkgs.dockerTools.buildLayeredImage {
        name = "glaredb";
        contents = [packages.cli];
        created = "now";
        config.Cmd = ["${packages.cli}/bin/glaredb"];
      };

      slt_runner = craneLib.buildPackage ({
        pname = "glaredb-slt-runner";
        inherit cargoArtifacts;
        cargoExtraArgs = "--bin slt_runner";
      } // common-build-args);
    };
  };
}
