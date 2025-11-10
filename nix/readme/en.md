# Build Kvrocks with Nix

This document provides instructions for building, running, and developing Kvrocks using the Nix package manager.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
  - [Build Kvrocks](#build-kvrocks)
  - [Run Kvrocks](#run-kvrocks)
- [Development](#development)
  - [Entering the Development Shell](#entering-the-development-shell)
- [NixOS Integration](#nixos-integration)
  - [System Configuration](#system-configuration)
  - [Deploying as a Service](#deploying-as-a-service)
- [A Note on Nix](#a-note-on-nix)

## Prerequisites

Ensure Nix is installed on your system. The flake-based workflow requires a Nix version that supports flakes.

Refer to the [official Nix installation guide](https://nixos.org/download.html).

## Quick Start

The project is packaged as a Nix flake, providing several outputs.

### Build Kvrocks

To compile the project, execute the `nix build` command from the project root directory.

```shell
nix build
```

This command builds the `kvrocks` package. The output is a symlink named `result` in the current directory, pointing to the build artifacts in the Nix store.

```shell
./result/bin/kvrocks --version
```

### Run Kvrocks

To compile and run `kvrocks` directly, use `nix run`.

```shell
nix run
```

Any arguments passed after `--` will be forwarded to the `kvrocks` executable.

```shell
nix run -- --help
```

## Development

For development, a reproducible shell is provided via `nix develop`.

### Entering the Development Shell

This shell contains all necessary dependencies and build tools, such as `cmake` and `gcc`.

```shell
nix develop
```

Inside this shell, you can use standard build commands.

```shell
cmake -S . -B build
cmake --build build
```

## NixOS Integration

For users on NixOS, Kvrocks can be integrated into the system configuration.

### System Configuration

Add the flake to your NixOS configuration's `inputs`.

```nix
# /etc/nixos/flake.nix
{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    kvrocks.url = "github:apache/kvrocks"; # Or your local path
  };

  outputs = { self, nixpkgs, kvrocks, ... }: {
    nixosConfigurations.your-hostname = nixpkgs.lib.nixosSystem {
      system = "x86_64-linux";
      modules = [
        ({ pkgs, ... }: {
          environment.systemPackages = [
            kvrocks.packages.${pkgs.system}.default
          ];
        })
        # ... other modules
      ];
    };
  };
}
```

Then, rebuild the system.

```shell
sudo nixos-rebuild switch --flake .#your-hostname
```

### Deploying as a Service

The flake provides a NixOS module to deploy Kvrocks as a systemd service.

To enable the service, add the module to your system configuration:

```nix
# /etc/nixos/flake.nix
{
  inputs = {
    # ...
    kvrocks.url = "github:apache/kvrocks";
  };

  outputs = { self, nixpkgs, kvrocks, ... }: {
    nixosConfigurations.your-hostname = nixpkgs.lib.nixosSystem {
      modules = [
        kvrocks.nixosModules.kvrocks
        # ... other modules
      ];
    };
  };
}
```

Then, enable the service in your configuration:

```nix
# /etc/nixos/configuration.nix
{
  services.kvrocks.enable = true;
}
```

#### Configuration

You can configure `kvrocks.conf` using the `services.kvrocks.settings` option. The keys are strings corresponding to the configuration keys in `kvrocks.conf`.

```nix
# /etc/nixos/configuration.nix
{
  services.kvrocks.enable = true;
  services.kvrocks.settings = {
    port = 6667;
    workers = 16;
    "rocksdb.write_buffer_size" = 256;
  };
}
```

For more complex configurations, you can provide a full `kvrocks.conf` file directly using the `services.kvrocks.configFile` option.

```nix
# /etc/nixos/configuration.nix
{
  services.kvrocks.enable = true;
  services.kvrocks.configFile = ./my-kvrocks.conf;
}
```

## A Note on Nix

Nix began as the PhD research of Eelco Dolstra in the early 2000s. The goal was to solve the fundamental problems of software deployment and dependency management, often called "dependency hell." The core idea was to treat package management with principles from functional programming: packages are built by pure functions, and the inputs to these functions (source code, dependencies, build scripts) are hashed to create a unique output path in the `/nix/store`. This ensures that builds are reproducible, and different versions of packages can coexist without conflict. This simple yet powerful concept led to the creation of Nix, the package manager, and subsequently NixOS, an entire Linux distribution built around these principles, offering declarative system configuration and reliable upgrades.
