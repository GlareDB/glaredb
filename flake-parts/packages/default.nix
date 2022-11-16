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

    # Utilities that are helpful to have in the container for debugging
    # purposes.
    #
    # For example, `coreutils` gives us `sleep` which can be useful to for
    # spinning up a debugging container on k8s:
    #
    # $ kubectl run my-test-container --restart=Never --image gcr.io/glaredb-dev-playground/glaredb@<image_sha> -- sleep inf
    # $ kubectl exec -it my-test-container -- bash
    debugPackages = [pkgs.coreutils pkgs.bash];

    common-build-args = rec {
      # crane arguments
      src = lib.cleanSourceWith {
        src = ../..;
        filter = self.lib.filterSrc craneLib;
      };
      pname = "glaredb";

      # application config arguments
      buildInputs = otherBuildInputs;
      nativeBuildInputs = otherNativeBuildInputs;
      # clang
      LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath buildInputs;
      BINDGEN_EXTRA_CLANG_ARGS = "-isystem ${pkgs.llvmPackages.libclang.lib}/lib/clang/${lib.getVersion pkgs.clang}/include";
      LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
      # protobuf compilation
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

    tests-check = craneLib.cargoNextest ({
      inherit cargoArtifacts;
      partitions = 1;
      partitionType = "count";
    } // common-build-args);

    fmt-check = craneLib.cargoFmt ({
      inherit cargoArtifacts;
    } // common-build-args);

    config-files = pkgs.stdenv.mkDerivation {
      name = "glaredb-config";

      src = ../../config;

      installPhase = ''
        mkdir -p $out/config
        cp -r * $out/config
      '';
    };
  in rec {
    checks = {
      inherit clippy-check tests-check fmt-check;
      build-crate = packages.default;
      build-sltrunner = packages.slt_runner;
    };
    packages = {
      default = packages.cli;

      config = config-files;

      cli = craneLib.buildPackage ({
        pname = "glaredb-cli";
        inherit cargoArtifacts;
        cargoExtraArgs = "--bin glaredb";
      } // common-build-args);

      glaredb_image = pkgs.dockerTools.buildLayeredImage {
        name = "glaredb";
        contents = [packages.cli pkgs.cacert config-files] ++ debugPackages;
        created = "now";
        config.Cmd = ["${packages.cli}/bin/glaredb"];
      };

      # Note that this currently uses the same command as the glaredb image. The
      # pgsrv proxy uses the same root command. Eventually this may be moved to
      # its own binary.
      #
      # Arguments will be provided in the k8s/terraform config. The api addr for
      # Cloud will be set to some internal uri.
      # See: https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/
      pgsrv_image = pkgs.dockerTools.buildLayeredImage {
        name = "pgsrv";
        contents = [packages.cli pkgs.cacert] ++ debugPackages;
        created = "now";
        config = {
          Cmd = ["${packages.cli}/bin/glaredb"];
        };
      };

      slt_runner = craneLib.buildPackage ({
        pname = "glaredb-slt-runner";
        inherit cargoArtifacts;
        cargoExtraArgs = "--bin slt_runner";
      } // common-build-args);
    };
  };
}
