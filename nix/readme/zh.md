# 使用 Nix 构建 Kvrocks

本文档提供使用 Nix 包管理器构建、运行和开发 Kvrocks 的说明。

## 目录

- [环境准备](#环境准备)
- [快速开始](#快速开始)
  - [构建 Kvrocks](#构建-kvrocks)
  - [运行 Kvrocks](#运行-kvrocks)
- [开发环境](#开发环境)
  - [进入开发 Shell](#进入开发-shell)
- [NixOS 集成](#nixos-集成)
  - [系统配置](#系统配置)
  - [部署为服务](#部署为服务)
- [关于 Nix 的小故事](#关于-nix-的小故事)

## 环境准备

确保系统中已安装 Nix。基于 Flake 的工作流需要支持 Flake 的 Nix 版本。

请参考 [Nix 官方安装指南](https://nixos.org/download.html)。

## 快速开始

项目被打包为一个 Nix Flake，提供了多种输出。

### 构建 Kvrocks

要编译项目，请在项目根目录中执行 `nix build` 命令。

```shell
nix build
```

此命令会构建 `kvrocks` 包。输出是当前目录下的一个名为 `result` 的符号链接，指向 Nix store 中的构建产物。

```shell
./result/bin/kvrocks --version
```

### 运行 Kvrocks

要直接编译并运行 `kvrocks`，请使用 `nix run`。

```shell
nix run
```

在 `--` 之后传递的任何参数都将转发给 `kvrocks` 可执行文件。

```shell
nix run -- --help
```

## 开发环境

为了方便开发，项目通过 `nix develop` 提供了一个可复现的 shell 环境。

### 进入开发 Shell

该 shell 包含了所有必需的依赖和构建工具，例如 `cmake` 和 `gcc`。

```shell
nix develop
```

在此 shell 中，可以使用标准构建命令。

```shell
cmake -S . -B build
cmake --build build
```

## NixOS 集成

对于 NixOS 用户，Kvrocks 可以集成到系统配置中。

### 系统配置

将此 Flake 添加到 NixOS 配置的 `inputs` 中。

```nix
# /etc/nixos/flake.nix
{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    kvrocks.url = "github:apache/kvrocks"; # 或你的本地路径
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
        # ... 其他模块
      ];
    };
  };
}
```

然后，重建系统。

```shell
sudo nixos-rebuild switch --flake .#your-hostname
```

### 部署为服务

该 Flake 提供了一个 NixOS 模块，用于将 Kvrocks 部署为 systemd 服务。

要启用该服务，请将模块添加到您的系统配置中：

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
        # ... 其他模块
      ];
    };
  };
}
```

然后，在您的配置中启用该服务：

```nix
# /etc/nixos/configuration.nix
{
  services.kvrocks.enable = true;
}
```

#### 配置

您可以使用 `services.kvrocks.settings` 选项来配置 `kvrocks.conf`。键是与 `kvrocks.conf` 中配置键对应的字符串。

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

对于更复杂的配置，您可以使用 `services.kvrocks.configFile` 选项直接提供一个完整的 `kvrocks.conf` 文件。

```nix
# /etc/nixos/configuration.nix
{
  services.kvrocks.enable = true;
  services.kvrocks.configFile = ./my-kvrocks.conf;
}
```

## 关于 Nix 的小故事

Nix 起源于 Eelco Dolstra 在 21 世纪初的博士研究。其目标是解决软件部署和依赖管理的根本性问题，即通常所说的“依赖地狱”。其核心思想是借鉴函数式编程的原理来处理包管理：包由纯函数构建，这些函数的输入（源代码、依赖、构建脚本）被哈希，以在 `/nix/store` 中创建一个唯一的输出路径。这确保了构建是可复现的，并且不同版本的软件包可以无冲突地共存。这个简单而强大的概念催生了 Nix 包管理器，并随后催生了 NixOS——一个完全围绕这些原则构建的 Linux 发行版，提供了声明式的系统配置和可靠的升级。
