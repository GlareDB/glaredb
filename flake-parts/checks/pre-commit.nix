{
  self,
  inputs,
  ...
} @ part-inputs: system:
  inputs.pre-commit-hooks.lib.${system}.run {
    src = self.lib.flake_source;
    hooks = {
      alejandra.enable = true;
      # rustfmt.enable = true;
      # clippy.enable = true;
    };
  }
