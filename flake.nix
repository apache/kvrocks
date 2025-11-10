{
  description = "A flake for kvrocks";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = {
    self,
    nixpkgs,
  }: let
    supportedSystems = ["x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin"];
    forEachSupportedSystem = f:
      nixpkgs.lib.genAttrs supportedSystems (system:
        f {
          pkgs = import nixpkgs {
            inherit system;
          };
        });

    dep = builtins.fromJSON (builtins.readFile ./nix/dep.json);
    sha = builtins.fromJSON (builtins.readFile ./nix/sha.json);
  in {
    packages = forEachSupportedSystem ({pkgs}: {
      default = pkgs.stdenv.mkDerivation (finalAttrs: let
        # Dynamically fetch all sources based on the JSON files
        sources = pkgs.lib.mapAttrs (name: info:
          pkgs.fetchFromGitHub {
            inherit (info) owner repo;
            rev = sha.${name}.rev;
            hash = sha.${name}.sha256;
          })
        dep;

        # Dynamically generate CMake flags for FetchContent
        fetchFlags =
          pkgs.lib.mapAttrsToList (
            name: src:
            # The variable name must be uppercase
            "-DFETCHCONTENT_SOURCE_DIR_${pkgs.lib.toUpper name}=${src}"
          )
          sources;
      in {
        pname = "kvrocks";
        version = builtins.readFile ./src/VERSION.txt;
        src = self;

        nativeBuildInputs =
          [
            pkgs.cmake
            pkgs.git
            pkgs.autoconf
          ]
          ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
            pkgs.cctools
          ];

        # Only include dependencies that are NOT fetched via FetchContent
        buildInputs =
          [
          ]
          ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
            pkgs.libiconv
          ];

        cmakeFlags =
          [
            "-DENABLE_STATIC_LIBSTDCXX=OFF"
            "-DDISABLE_JEMALLOC=OFF"
            "-DCMAKE_BUILD_TYPE=Release"
          ]
          ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
            "-DCPPTRACE_ADDR2LINE_PATH_FINAL=${pkgs.cctools}/bin/atos"
          ]
          ++ fetchFlags;

        # Set optimization flags via environment variables instead of cmakeFlags
        # to avoid parsing issues with spaces
        NIX_CFLAGS_COMPILE = "-O3";
        CXXFLAGS = "-O3";
        CFLAGS = "-O3";

        # Enable parallel building
        enableParallelBuilding = true;

        # Ensure build directory exists and is writable
        # Also make the source tree writable for dependencies that build in-source
        postUnpack = ''
          chmod -R +w $sourceRoot
        '';

        preConfigure = ''
          mkdir -p build
        '';

        installPhase = ''
          runHook preInstall

          mkdir -p $out/bin
          cp kvrocks $out/bin/
          cp kvrocks2redis $out/bin/

          runHook postInstall
        '';

        meta = with pkgs.lib; {
          description = "A distributed key-value NoSQL database that uses RocksDB as storage engine and is compatible with Redis protocol";
          homepage = "https://kvrocks.apache.org/";
          license = licenses.asl20;
          maintainers = with maintainers; [];
          platforms = platforms.linux ++ platforms.darwin;
        };
      });
    });

    nixosModules.kvrocks = {
      config,
      lib,
      pkgs,
      ...
    }: let
      cfg = config.services.kvrocks;
      # Convert an attribute set to a string suitable for kvrocks.conf
      toKeyValue = attrs:
        lib.concatStringsSep "\n"
        (lib.mapAttrsToList (
            k: v: let
              v' =
                if lib.isList v
                then lib.concatStringsSep " " v
                else if lib.isBool v
                then
                  if v
                  then "yes"
                  else "no"
                else toString v;
            in "${k} ${v'}"
          )
          attrs);
      configFileFromSettings = pkgs.writeText "kvrocks.conf" (toKeyValue (lib.recursiveUpdate {
          # Default settings for systemd service
          daemonize = "no";
          dir = "/var/lib/kvrocks";
          pidfile = "/run/kvrocks/kvrocks.pid";
          "log-dir" = "/var/log/kvrocks";
        }
        cfg.settings));
    in {
      options.services.kvrocks = {
        enable = lib.mkEnableOption "kvrocks server";

        package = lib.mkOption {
          type = lib.types.package;
          default = self.packages.${pkgs.system}.default;
          defaultText = lib.literalExpression "self.packages.${pkgs.system}.default";
          description = "The kvrocks package to use.";
        };

        configFile = lib.mkOption {
          type = with lib.types; nullOr path;
          default = null;
          description = ''
            Path to the `kvrocks.conf` file.
            If set, this will be used directly. Otherwise, the configuration
            will be generated from `settings`.
          '';
        };

        settings = lib.mkOption {
          type = with lib.types;
            attrsOf (oneOf [
              str
              int
              bool
              (listOf str)
            ]);
          default = {};
          example = lib.literalExpression ''
            {
              port = 6666;
              bind = "127.0.0.1";
              "rocksdb.write_buffer_size" = 128;
            }
          '';
          description = "kvrocks configuration. See kvrocks.conf for details.";
        };
      };

      config = lib.mkIf cfg.enable {
        environment.etc."kvrocks/kvrocks.conf" = {
          source =
            if cfg.configFile != null
            then cfg.configFile
            else configFileFromSettings;
          owner = "kvrocks";
          group = "kvrocks";
          mode = "0440";
        };

        systemd.tmpfiles.rules = [
          "d /var/lib/kvrocks 0750 kvrocks kvrocks -"
          "d /var/log/kvrocks 0750 kvrocks kvrocks -"
        ];

        users.users.kvrocks = {
          isSystemUser = true;
          group = "kvrocks";
          home = "/var/lib/kvrocks";
        };
        users.groups.kvrocks = {};

        systemd.services.kvrocks = {
          description = "kvrocks server";
          after = ["network.target"];
          wantedBy = ["multi-user.target"];

          serviceConfig = {
            Type = "simple";
            User = "kvrocks";
            Group = "kvrocks";
            ExecStart = "${cfg.package}/bin/kvrocks -c /etc/kvrocks/kvrocks.conf";
            PIDFile = "/run/kvrocks/kvrocks.pid";
            RuntimeDirectory = "kvrocks";
            RuntimeDirectoryMode = "0755";
            StandardOutput = "journal";
            StandardError = "journal";
            ReadWritePaths = [
              "/var/lib/kvrocks"
              "/var/log/kvrocks"
            ];
          };
        };
      };
    };
  };
}
