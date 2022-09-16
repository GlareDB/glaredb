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
    };
  };
}
