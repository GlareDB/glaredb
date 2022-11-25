{
  inputs,
  self,
  lib,
  ...
}: {
  perSystem = {
    pkgs,
    system,
    inputs',
    ...
  }: let
    # run-time dependencies
    otherBuildInputs = [
        pkgs.openssl
        pkgs.protobuf
    ];

    # build-time dependencies
    otherNativeBuildInputs = [
      pkgs.pkgconfig
      pkgs.openssl
      pkgs.openssl.dev
      pkgs.clang
      pkgs.llvmPackages.bintools
      pkgs.llvmPackages.libclang
    ] ++ lib.optional pkgs.stdenv.isDarwin [
      # for macOS
      pkgs.libiconv
      pkgs.darwin.apple_sdk.frameworks.Security
    ];

    # development dependencies
    devTools = [
      pkgs.rustfmt
      pkgs.bacon
      pkgs.cargo-udeps
      pkgs.cargo-nextest
      pkgs.cocogitto
      pkgs.protobuf
      pkgs.gdb
      pkgs.postgresql
    ];

    # dependencies available for a CI environment
    ciPackages = [
      (pkgs.google-cloud-sdk.withExtraComponents ([pkgs.google-cloud-sdk.components.gke-gcloud-auth-plugin])) 
      pkgs.kubectl
      pkgs.skopeo
    ];

    # Utilities that are helpful to have in the container for debugging
    # purposes.
    #
    # For example, `coreutils` gives us `sleep` which can be useful to for
    # spinning up a debugging container on k8s:
    #
    # $ kubectl run my-test-container --restart=Never --image gcr.io/glaredb-dev-playground/glaredb@<image_sha> -- sleep inf
    # $ kubectl exec -it my-test-container -- bash
    debugPackages = [pkgs.coreutils pkgs.bash];

    # pin to rust-stable - see https://github.com/nix-community/fenix
    fenixChannel = inputs'.fenix.packages.stable;
    fenixToolchain = fenixChannel.withComponents [
      "rustc"
      "cargo"
      "clippy"
      "rust-analysis"
      "rust-src"
      "rustfmt"
      "llvm-tools-preview"
    ];

    # configure crane using the pinned toolchain
    craneLib = inputs.crane.lib.${system}.overrideToolchain fenixToolchain;

    # common build-time configuration
    common-build-args = rec {
      # only include relevant files to avoid rebuilds when changing unrelated files
      src = lib.cleanSourceWith {
        src = ../.;
      };
      pname = "glaredb";

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

    # build cargo dependencies only once if they don't change
    deps-only = craneLib.buildDepsOnly ({} // common-build-args);

    # rust linting
    clippy-check = craneLib.cargoClippy ({
      cargoArtifacts = deps-only;
      cargoClippyExtraArgs = "--all-features -- --deny warnings";
    } // common-build-args);

    # rust tests
    tests-check = craneLib.cargoNextest ({
      cargoArtifacts = deps-only;
      partitions = 1;
      partitionType = "count";
    } // common-build-args);

    # rustfmt
    fmt-check = craneLib.cargoFmt ({
      cargoArtifacts = deps-only;
    } // common-build-args);

    crate-docs = craneLib.cargoDoc ({
      cargoArtifacts = deps-only;
    } // common-build-args);

    # The included default configuration
    config-directory = pkgs.stdenv.mkDerivation {
      name = "glaredb-config";

      src = .././config;

      installPhase = ''
        mkdir -p $out/config
        cp -r * $out/config
      '';
    };

    # utility for creating container images with common arguments
    mkContainer = { name, contents, config, ... }:
      pkgs.dockerTools.buildLayeredImage {
        inherit name;
        tag = self.rev or "dirty";
        contents = contents ++ debugPackages;
        created = "now";

        inherit config;
      };
  in rec {
    checks = {
      # ensure crane's checks pass
      inherit clippy-check tests-check fmt-check;

      # ensure the packages can be built
      build-crate = packages.default;
      build-sltrunner = packages.slt_runner;
    };

    packages = {
      default = packages.cli;

      config = config-directory;

      crate_docs = crate-docs;

      cli = craneLib.buildPackage ({
        pname = "glaredb-cli";
        cargoArtifacts = deps-only;
        cargoExtraArgs = "--bin glaredb";
      } // common-build-args);

      glaredb_image = mkContainer {
        name = "glaredb";
        contents = [pkgs.cacert packages.config packages.cli];
        config.Cmd = ["${packages.cli}/bin/glaredb"];
      };

      # Note that this currently uses the same command as the glaredb image. The
      # pgsrv proxy uses the same root command. Eventually this may be moved to
      # its own binary.
      #
      # Arguments will be provided in the k8s/terraform config. The api addr for
      # Cloud will be set to some internal uri.
      # See: https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/
      pgsrv_image = mkContainer {
        name = "pgsrv";
        contents = [pkgs.cacert packages.cli];
        config.Cmd = ["${packages.cli}/bin/glaredb"];
      };

      slt_runner = craneLib.buildPackage ({
        pname = "glaredb-slt-runner";
        cargoArtifacts = deps-only;
        cargoExtraArgs = "--bin slt_runner";
      } // common-build-args);

      pgprototest = craneLib.buildPackage ({
        pname = "glaredb-pgprototest";
        cargoArtifacts = deps-only;
        cargoExtraArgs = "--bin pgprototest";
      } // common-build-args);
    };

    # provide runnable programs
    apps = {
      cli = {
        type = "app";
        program = "${packages.cli}/bin/glaredb";
      };
      slt_runner = {
        type = "app";
        program = "${packages.slt_runner}/bin/slt_runner";
      };
      pgprototest = {
        type = "app";
        program = "${packages.pgprototest}/bin/pgprototest";
      };
      default = apps.cli;
    };

    devShells = {
      # Provide a shell with the pinned toolchain and all dependencies
      # a developer might need.
      default = pkgs.mkShell rec {
        buildInputs = [fenixToolchain] ++ devTools ++ ciPackages ++ otherBuildInputs;
        nativeBuildInputs = otherNativeBuildInputs;

        # Set up environment variables for tools that need them
        LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath buildInputs;
        inherit (common-build-args) BINDGEN_EXTRA_CLANG_ARGS LIBCLANG_PATH;
        inherit (common-build-args) PROTOC PROTOC_INCLUDE;
      };
      # restrict the dependencies provided to save on ci time
      ci = pkgs.mkShell rec {
        buildInputs = ciPackages;
      };
    };

  };
}
