{
  inputs,
  self,
  ...
} @ part-inputs: {
  perSystem = {
    config,
    pkgs,
    system,
    ...
  }: let
    rust-stable = self.lib.rust-stable system;
    rust-nightly = self.lib.rust-nightly system;
    devTools = with pkgs; [
      rustfmt
      bacon
      cargo-udeps
      cocogitto
    ];
  in rec {
    devShells = {
      default = devShells.nightly;
      stable = pkgs.mkShell rec {
        buildInputs = [rust-stable] ++ devTools;
        inherit (self.checks.${system}.pre-commit) shellHook;
      };
      nightly = pkgs.mkShell rec {
        buildInputs = [rust-nightly] ++ devTools;
        inherit (self.checks.${system}.pre-commit) shellHook;
      };
      postgres = with pkgs;
        mkShell rec {
          buildInputs = [postgresql rust-stable];
          shellHook = ''
            ${self.checks.${system}.pre-commit.shellHook}
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
