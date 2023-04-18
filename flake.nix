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
        craneLibFenix = crane.lib.${system}.overrideToolchain fenixToolchain;

        craneLib = (craneLibFenix).overrideScope' (final: prev: {
          # We override the behavior of `mkCargoDerivation` by adding a wrapper which
          # will set a default value of `CARGO_PROFILE` when not set by the caller.
          # This change will automatically be propagated to any other functions built
          # on top of it (like `buildPackage`, `cargoBuild`, etc.)
          mkCargoDerivation = args: prev.mkCargoDerivation ({
            CARGO_PROFILE = ""; # Unset profile (crane defaults to release)
          } // args);
        });

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
        common-build-args = {
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
        #
        # http://ryantm.github.io/nixpkgs/builders/images/dockertools/
        mkContainer = { name, contents, config, ... }:
          pkgs.dockerTools.buildImage {
            inherit name config;
            tag = self.rev or "dirty";
            created = "now";
            copyToRoot = with pkgs.dockerTools; [
              usrBinEnv
              binSh
              caCertificates
              fakeNss
            ] ++ containerDebugPackages ++ contents;
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

        # GlareDB binary.
        #
        # This is also used for cargo artifacts for downstream targets (since
        # this pretty much builds everything).
        glaredb-bin = craneLib.buildPackage (common-build-args // {
          pname = "glaredb";
          cargoExtraArgs = "--bin glaredb";
          doCheck = false;
          doInstallCargoArtifacts = true;
        });

        # GlareDB image (with release).
        glaredb-image = mkContainer {
          name = "glaredb";
          contents = [
            pkgs.openssh
            glaredb-bin
            # Generated certs used for SSL connections in pgsrv. GlareDB
            # proper does not currently use certs.
            generated-certs
          ];
          config.Cmd = ["${glaredb-bin}/bin/glaredb"];
        };

        # SLT runner binary.
        slt-runner-bin = craneLib.buildPackage (common-build-args // {
          cargoArtifacts = glaredb-bin;
          pname = "slt-runner";
          cargoExtraArgs = "--bin slt_runner";
          doCheck = false;
        });

        # pgprototest binary.
        pgprototest-bin = craneLib.buildPackage (common-build-args // {
          cargoArtifacts = glaredb-bin;
          pname = "pgprototest";
          cargoExtraArgs = "--bin pgprototest";
          doCheck = false;
        });

      in rec {
        # Checks ran in CI. This includes linting and testing.
        checks = {
          inherit glaredb-bin;
          inherit slt-runner-bin;
          inherit pgprototest-bin;

          # Run tests.
          tests-check = craneLib.cargoNextest (common-build-args // {
            cargoArtifacts = glaredb-bin;
            partitions = 1;
            partitionType = "count";
          });

          # Run clippy.
          clippy-check = craneLib.cargoClippy (common-build-args // {
            cargoArtifacts = glaredb-bin;
            cargoClippyExtraArgs = "--all-features -- --deny warnings";
          });

          # Check formatting.
          fmt-check = craneLib.cargoFmt (common-build-args // {
            cargoArtifacts = glaredb-bin;
          });

          # Check docs (and doc tests).
          crate-docs = craneLib.cargoDoc (common-build-args // {
            cargoArtifacts = glaredb-bin;
          });
        };

        # Buildable packages.
        packages = {
          inherit generated-certs;
          inherit glaredb-bin;
          inherit glaredb-image;
          inherit slt-runner-bin;
          inherit pgprototest-bin;
        };

        # Runnable applications.
        apps = {
          glaredb = {
            type = "app";
            program = "${glaredb-bin}/bin/glaredb";
          };
          slt-runner = {
            type = "app";
            program = "${slt-runner-bin}/bin/slt_runner";
          };
          pgprototest = {
            type = "app";
            program = "${pgprototest-bin}/bin/pgprototest";
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
