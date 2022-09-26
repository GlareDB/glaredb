{
  inputs,
  self,
  ...
} @ part-inputs: {
  perSystem = {
    config,
    pkgs,
    system,
    inputs',
    ...
  }: let
    rust-stable = inputs'.fenix.packages.stable.toolchain;
    rust-nightly = inputs'.fenix.packages.latest.toolchain;

    devTools = with pkgs; [
      rustfmt
      bacon
      cargo-udeps
      cargo-nextest
      cocogitto
      protobuf
      gdb
    ];

    otherNativeBuildInputs = with pkgs; [pkgconfig openssl openssl.dev llvmPackages.bintools];
    otherBuildInputs = with pkgs; [
      clang
    ];

    LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
    PROTOC = "${pkgs.protobuf}/bin/protoc";
    PROTOC_INCLUDE = "${pkgs.protobuf}/include";
  in rec {
    devShells = {
      default = devShells.nightly;
      stable = pkgs.mkShell rec {
        buildInputs = [rust-stable] ++ devTools ++ otherBuildInputs;
        nativeBuildInputs = otherNativeBuildInputs;
        LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath buildInputs;
        inherit LIBCLANG_PATH;
        inherit PROTOC PROTOC_INCLUDE;
      };
      nightly = pkgs.mkShell rec {
        buildInputs = [rust-nightly] ++ devTools ++ otherBuildInputs;
        nativeBuildInputs = otherNativeBuildInputs;
        LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath buildInputs;
        inherit LIBCLANG_PATH;
        inherit PROTOC PROTOC_INCLUDE;
      };
      postgres = with pkgs;
        mkShell rec {
          buildInputs = [postgresql rust-stable];
          shellHook = ''
            export PGDATA="$PWD/db"
            export PGHOST="$PGDATA"
            export PGLOG=$PGDATA/postgres.log

            if [ ! -d $PGDATA ]; then
                mkdir -p $PGDATA
                initdb -D $PGDATA --auth=trust --no-locale --encoding=UTF8
                createuser postgres
            fi

            if ! pg_ctl status
            then
                pg_ctl start -l $PGLOG -o "--unix-socket-directories='$PGDATA'"
            fi

            function end {
                echo "Shutting down postgres..."
                pg_ctl stop
            }

            trap end EXIT
          '';
        };
    };
  };
}
