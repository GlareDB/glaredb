{
  description = "GlareDB";

  inputs = {
    nixpkgs.url = "nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    flake-parts = {
      url = "github:hercules-ci/flake-parts";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    pre-commit-hooks.url = "github:cachix/pre-commit-hooks.nix";
    gitignore = {
      url = "github:hercules-ci/gitignore.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
    flake-parts,
    gitignore,
    rust-overlay,
    pre-commit-hooks,
    ...
  }:
    flake-parts.lib.mkFlake {inherit self;} {
      perSystem = {
        config,
        self',
        inputs',
        pkgs,
        system,
        ...
      }: let
        inherit (gitignore.lib) gitignoreSource;
        pre-commit-check = pre-commit-hooks.lib.${system}.run {
          src = gitignoreSource ./.;
          hooks = {
            alejandra.enable = true;
          };
        };

        opkgs = import nixpkgs {
          inherit system;
          overlays = [
            rust-overlay.overlays.default
          ];
        };
        rust-stable = opkgs.rust-bin.stable.latest.default;
        rust-nightly = opkgs.rust-bin.nightly.latest.default;
        shellInputs = with pkgs; [
          rustfmt
          bacon
          cargo-udeps
          miniserve
        ];
      in rec {
        devShells = {
          default = pkgs.mkShell rec {
            buildInputs = [rust-stable] ++ shellInputs;
            inherit (pre-commit-check) shellHook;
          };
          nightly = pkgs.mkShell rec {
            buildInputs = [rust-nightly] ++ shellInputs;
            inherit (pre-commit-check) shellHook;
          };
        };
      };
      systems = flake-utils.lib.defaultSystems;
    };
}
