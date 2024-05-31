{
  description = "A fast SQL database for running analytics across distributed data";
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        version = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).workspace.package.version;
      in
      {
        formatter = pkgs.nixpkgs-fmt;
        packages.default = pkgs.rustPlatform.buildRustPackage {
          name = "glaredb";
          version = "${version}-${self.shortRev or "dirty"}";
          src = ./.;
          doCheck = false;
          nativeBuildInputs = with pkgs; [ protobuf ];
          buildAndTestSubdir = "crates/cli";
          preBuild = ''
            export PROTOC=${pkgs.protobuf}/bin/protoc
          '';
          cargoLock = {
            lockFile = ./Cargo.lock;
            outputHashes = {
              "bigquery-storage-0.1.2" = "sha256-o23/ALoVtE7SqJsxmXQnykxzqrVZ+Se/gXFYIlIskkI=";
              "deltalake-0.17.0" = "sha256-p+k/MXimI7HDGvmrk6WMNhf2MbLrG7B9RlESSyhV04o=";
              "lance-0.9.12" = "sha256-vWyXxoRDStvbToubciQSQgl/PErmM333UUGDsjhgr0k=";
            };
          };
          meta = with pkgs.lib; {
            description = "A fast SQL database for running analytics across distributed data";
            homepage = "https://github.com/glaredb/glaredb";
            license = licenses.agpl3Only;
          };
        };
        devShells.default = with pkgs; mkShell {
          packages = [ cargo rustc rust-analyzer rustfmt ];
          RUST_SRC_PATH = rustPlatform.rustLibSrc;
        };
      }
    );
}
