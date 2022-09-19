# Contributing to GlareDB

All commits to `main` should first go through a PR. All CI checks should pass
before merging in a PR. Since we squash merge, PR titles should follow
[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).

Development dependencies may (optionally) be installed using nix.
By using nix all programs needed (cargo, protoc, &c) will be installed and you will be placed into a shell configured with them in your PATH.

## nix

### Obtaining a development environment

First, if you are not using NixOS you must install nix and configure the experimental flakes feature.

The [nix download page](https://nixos.org/download.html) has instructions on installation.
To enable flakes, add the following to your `nix.conf` in either `~/.config/nix/nix.conf` or `/etc/nix/nix.conf` 
```
experimental-features = nix-command flakes
```
The nixos wiki has [instructions for enabling flakes](https://nixos.wiki/wiki/Flakes#Enable_flakes) which should have the most up to date information.

When all of these steps are complete, you should be able to obtain the projects development shell: `nix develop`
From this shell you may work on the project and use any cli tools needed.

### other information

This project uses [flake-parts](https://github.com/hercules-ci/flake-parts) to organize the nix flake output into multiple files.
`flake.nix` is kept minimal and the [flake outputs](https://nixos.wiki/wiki/Flakes#Output_schema) are gathered from subdirectories that share their names inside the `flake-parts` directory.
Insight into each of these may be gained by looking at the nix source and/or their READMEs.

### nix commands

| command | description | relevant flake part path |
| ------- | ----------- | --------------- |
| `nix develop` | obtain development shell | [flake-parts/devshell](flake-parts/devshell) |
| `nix build` | build packages (binaries, container images, &c) | [flake-parts/packages](flake-parts/packages) |
| `nix flake show` | print all outputs provided by the flake. useful since the project is organized into multiple files | N/A |

### direnv

[nix-direnv](https://github.com/nix-community/nix-direnv) works especially well with nix devshells.
If you install it you may place a `.envrc` in your local workspace with the contents: `use flake` and
the project's devshell will automatically be entered when inside its directory.
