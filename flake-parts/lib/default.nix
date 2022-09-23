{
  inputs,
  self,
  lib,
  ...
}: let
  inherit (inputs.gitignore.lib) gitignoreSource;
in {
  flake.lib = {
    otherNativeBuildInputs = pkgs: with pkgs; [pkgconfig openssl openssl.dev llvmPackages.bintools llvmPackages.libclang];
    otherBuildInputs = pkgs:
      with pkgs; [
        clang
        openssl
        protobuf
      ];

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
