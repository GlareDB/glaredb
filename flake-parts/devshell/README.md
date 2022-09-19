# devshells

This defines a number of development shells.
For most purposes you will just be able to use the default shell: `nix develop`

Other shells may be specified by name, using the syntax `nix develop .#{name}`

## project shells

The main shells for this project are: `stable` and `nightly`.
These correspond to which version of the rust toolchain is used.
The default devshell is set to nightly, which is useful for tooling such as clippy or cargo-udeps.

### included packages

A set of tools are installed to help with development
- [bacon](https://github.com/Canop/bacon)
- [cocogitto](https://github.com/cocogitto/cocogitto)
- [cargo-udeps](https://github.com/est31/cargo-udeps)

Other dependencies may be added by finding them on [nixpkgs search](https://search.nixos.org/packages?channel=unstable) and adding them to the `devTools` list.

## postgres shell

`nix develop .#postgres`

This is a specialized devshell which will start an instance of postgres while the shell is active.
Tools needed to interact with it (namely, psql) are included
