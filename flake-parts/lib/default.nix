{
  inputs,
  self,
  lib,
  ...
}: let
  inherit (inputs.gitignore.lib) gitignoreSource;
in {
  flake.lib = {
    # Filters for files to include in nix store
    filterProto = orig_path: type:
      let
        path = (toString orig_path);
        base = baseNameOf path;

        matchesSuffix = lib.any (suffix: lib.hasSuffix suffix base) [
          # Include all proto files
          ".proto"
        ];
      in
        type == "directory" || matchesSuffix;

    filterSrc = craneLib: orig_path: type:
      (self.lib.filterProto orig_path type) || (craneLib.filterCargoSources orig_path type);
  };
}
