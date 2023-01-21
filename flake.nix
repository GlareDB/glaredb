{
  description = "GlareDB";

  inputs = {
    nixpkgs.url = "nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";

    # Allows us to pick a rust toolchain independent of nixpkgs.
    #
    # Udpating can be done via `nix flake lock --update-input fenix`
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    # Utilities for interacting with cargo based projects.
    crane = {
      url = "github:ipetkov/crane";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, fenix, crane,  ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [fenix.overlays.default];
        };

        inherit (pkgs) lib;

        # Pin to rust-stable - see https://github.com/nix-community/fenix
        fenixChannel = pkgs.fenix.stable;
        fenixToolchain = fenixChannel.withComponents [
          "rustc"
          "cargo"
          "clippy"
          "rust-src"
          "rustfmt"
        ];

        # Configure crane using the pinned toolchain.
        craneLib = crane.lib.${system}.overrideToolchain fenixToolchain;

        # Run-time dependencies.
        buildInputs = [
          pkgs.openssl
        ];

        # Build-time dependencies.
        nativeBuildInputs = [
          pkgs.git # For getting tag/hash at build time.
          pkgs.pkgconfig
          pkgs.protobuf
          pkgs.openssl
          pkgs.openssl.dev
          pkgs.clang
          pkgs.llvmPackages.bintools
          pkgs.llvmPackages.libclang
        ] ++ lib.optional pkgs.stdenv.isDarwin [
          # Packages required to build on mac.
          pkgs.libiconv
          pkgs.darwin.apple_sdk.frameworks.Security
        ];

        # Filter function to match protobuf files.
        filterProto = path: _type: builtins.match ".*proto$" path != null;
        # Filter function to match all source files (protobuf, cargo, rust).
        filterSources = path: type:
          (filterProto path type) || (craneLib.filterCargoSources path type);

        # Common configuration for all builds.
        #
        # This tracks our build inputs and the source files we'll be building with.
        common-build-args = rec {
          inherit buildInputs nativeBuildInputs;

          # Filter source to to only include the files we care about for
          # building.
          src = lib.cleanSourceWith {
            src = ./.;
            filter = filterSources;
          };

          # The `prost` crate uses `protoc` for parsing protobuf files. Ensure it
          # can find the one we're providing with nix.
          PROTOC = "${pkgs.protobuf}/bin/protoc";

          # Used during build time to inject the git revision into the binary.
          #
          # See the `buildenv` crate.
          GIT_TAG_OVERRIDE = self.rev or "dirty";
        };

        # Build all dependencies. Built dependencies will be reused across checks
        # and builds.
        cargoArtifacts = craneLib.buildDepsOnly ({} // common-build-args);

        # Derivation for generating and including SSL certs.
        generated-certs = pkgs.stdenv.mkDerivation {
          name = "generated-certs";
          buildInputs = [pkgs.openssl pkgs.coreutils];
          src = ./.;
          installPhase = "./scripts/gen-certs.sh && mkdir -p $out/certs && cp server.{crt,key} $out/certs/.";
        };

        # Utilities that are helpful to have in the container for debugging
        # purposes.
        #
        # For example, `coreutils` gives us `sleep` which can be useful to for
        # spinning up a debugging container on k8s:
        #
        # $ kubectl run my-test-container --restart=Never --image gcr.io/glaredb-dev-playground/glaredb@<image_sha> -- sleep inf
        # $ kubectl exec -it my-test-container -- bash
        containerDebugPackages = [pkgs.coreutils pkgs.bash];

        # Function for creating container images with common arguments. The
        # resulting container will include the provided contents as well as the
        # debug package specified in `containerDebugPackages`.
        mkContainer = { name, contents, config, ... }:
          pkgs.dockerTools.buildLayeredImage {
            inherit name config;
            tag = self.rev or "dirty";
            contents = contents ++ containerDebugPackages;
            created = "now";
          };

        # Packages included in the 'ci' shell.
        ciPackages = [
          (pkgs.google-cloud-sdk.withExtraComponents ([pkgs.google-cloud-sdk.components.gke-gcloud-auth-plugin])) 
          pkgs.kubectl
          pkgs.skopeo
        ];

        # Packages included in the default dev shell. This includes everything
        # needed to build plus some extras.
        devPackages =
          [(fenixChannel.withComponents [
            "rustc"
            "cargo"
            "clippy"
            "rust-src"
            "rustfmt"
            "rust-analyzer"
          ])]
          ++ ciPackages
          ++ buildInputs
          ++ nativeBuildInputs;

      in rec {
        # Checks ran in CI. This includes linting and testing.
        checks = {
          # Ensure the packages can be built.
          glaredb-bin = packages.glaredb-bin;
          slt-runner-bin = packages.slt-runner-bin;
          pgprototest-bin = packages.pgprototest-bin;

          # Run clippy.
          clippy-check = craneLib.cargoClippy ({
            inherit cargoArtifacts;
            cargoClippyExtraArgs = "--all-features -- --deny warnings";
          } // common-build-args);

          # Run tests.
          tests-check = craneLib.cargoNextest ({
            inherit cargoArtifacts;
            partitions = 1;
            partitionType = "count";
          } // common-build-args);

          # Check formatting.
          fmt-check = craneLib.cargoFmt ({
            inherit cargoArtifacts;
          } // common-build-args);

          # Check docs (and doc tests).
          crate-docs = craneLib.cargoDoc ({
            inherit cargoArtifacts;
          } // common-build-args);
        };

        # Buildable packages.
        packages = {
          generated-certs = generated-certs;

          # GlareDB binary.
          glaredb-bin = craneLib.buildPackage ({
            inherit cargoArtifacts;
            pname = "glaredb";
            cargoExtraArgs = "--bin glaredb";
          } // common-build-args);

          # GlareDB image.
          glaredb-image = mkContainer {
            name = "glaredb";
            contents = [
              pkgs.cacert
              packages.glaredb-bin
              # Generated certs used for SSL connections in pgsrv. GlareDB
              # proper does not currently use certs.
              generated-certs
            ];
            config.Cmd = ["${packages.glaredb-bin}/bin/glaredb"];
          };

          # Pgsrv image.
          #
          # Note that this currently uses the same command as the glaredb image. The
          # pgsrv proxy uses the same root command. Eventually this may be moved to
          # its own binary.
          #
          # Arguments will be provided in the k8s/terraform config. The api addr for
          # Cloud will be set to some internal uri.
          # See: https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/
          pgsrv-image = mkContainer {
            name = "pgsrv";
            contents = [pkgs.cacert packages.glaredb-bin generated-certs];
            config.Cmd = ["${packages.glaredb-bin}/bin/glaredb"];
          };

          slt-runner-bin = craneLib.buildPackage ({
            inherit cargoArtifacts;
            pname = "slt-runner";
            cargoExtraArgs = "--bin slt_runner";
          } // common-build-args);

          pgprototest-bin = craneLib.buildPackage ({
            inherit cargoArtifacts;
            pname = "pgprototest";
            cargoExtraArgs = "--bin pgprototest";
          } // common-build-args);
        };

        # Runnable applications.
        apps = {
          glaredb = {
            type = "app";
            program = "${packages.glaredb-bin}/bin/glaredb";
          };
          slt-runner = {
            type = "app";
            program = "${packages.slt-runner-bin}/bin/slt_runner";
          };
          pgprototest = {
            type = "app";
            program = "${packages.pgprototest-bin}/bin/pgprototest";
          };
          default = apps.glaredb;
        };

        devShells = {
          # Provide a shell with the pinned toolchain and all dependencies
          # a developer might need.
          default = pkgs.mkShell {
            inherit (common-build-args) PROTOC;
            buildInputs = devPackages;
          };

          # Restrict the dependencies provided to save on ci time.
          ci = pkgs.mkShell {
            inherit (common-build-args) PROTOC;
            buildInputs = ciPackages;
          };
        };
      });
}
