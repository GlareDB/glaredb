{
  inputs,
  self,
  ...
}: {
  perSystem = {
    config,
    pkgs,
    system,
    ...
  }: rec {
    apps = {
      cli = {
        type = "app";
        program = "${self.packages.${system}.cli}/bin/glaredb";
      };
      slt_runner = {
        type = "app";
        program = "${self.packages.${system}.slt_runner}/bin/slt_runner";
      };
      default = apps.cli;
    };
  };
}
