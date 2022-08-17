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
  }: {
    checks = {
      pre-commit = import ./pre-commit.nix part-inputs system;
    };
  };
}
