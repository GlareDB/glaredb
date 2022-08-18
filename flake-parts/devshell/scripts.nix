{pkgs, ...} @ per-system-inputs: let
  cog = "${pkgs.cocogitto}/bin/cog";

  get-version-script = pkgs.writeShellScriptBin "get-version" ''
    git describe --tags "$(git rev-list --tags --max-count=1)"
  '';
  get-version = "${get-version-script}/bin/get-version";
in {
  ci = {
    get-version = get-version-script;
    bump-version = pkgs.writeShellScriptBin "bump-version" ''
      ${cog} bump --auto || exit 1
      VERSION=`${get-version}`
      echo ::set-output name=version::"$VERSION"
    '';

    check-commits = pkgs.writeShellScriptBin "check-commits" ''
      ${cog} check --from-latest-tag || exit 1
    '';

    generate-changelog = pkgs.writeShellScriptBin "generate-changelog" ''
      ${cog} changelog --at ''${1} -t full_hash
    '';

    print-version = pkgs.writeShellScriptBin "print-version" ''
      echo `${get-version}`
    '';
  };
}
