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
      default = apps.cli;
    };
  };
}
