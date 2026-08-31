{
  description = "CLN descriptor tracker plugin";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    cln-bwatch = {
      url = "github:niftynei/lightning/c902a7d4a11204fd78b754339f091a2c67f7715f";
      flake = false;
    };
    cln-gheap = {
      url = "github:valyala/gheap/67fc83bc953324f4759e52951921d730d7e65099";
      flake = false;
    };
    cln-jsmn = {
      url = "github:zserge/jsmn/18e9fe42cbfe21d65076f5c77ae2be379ad1270f";
      flake = false;
    };
    cln-libbacktrace = {
      url = "github:ianlancetaylor/libbacktrace/793921876c981ce49759114d7bb89bb89b2d3a2d";
      flake = false;
    };
    cln-libwally = {
      url = "github:ElementsProject/libwally-core/0c41f38fb1c201786e9c3ac9eae4f5f80c051399";
      flake = false;
    };
    cln-secp256k1 = {
      url = "github:BlockstreamResearch/secp256k1-zkp/45f6f0f158c5ae80a2c8a53398ea4adbf19af6dc";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, cln-bwatch, cln-gheap, cln-jsmn, cln-libbacktrace, cln-libwally, cln-secp256k1 }:
    let
      systems = [ "aarch64-darwin" "x86_64-darwin" "aarch64-linux" "x86_64-linux" ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
      perSystem = system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          cln-source = cln-bwatch;

          tracker = pkgs.rustPlatform.buildRustPackage {
            pname = "cln-tracker";
            version = "0.1.0";
            src = pkgs.lib.fileset.toSource {
              root = ./.;
              fileset = pkgs.lib.fileset.unions [
                ./Cargo.lock
                ./Cargo.toml
                ./src
              ];
            };
            cargoLock.lockFile = ./Cargo.lock;
            nativeBuildInputs = [ pkgs.pkg-config ];
            doCheck = true;
          };

          # CLN's normal package builds manuals and installs its Rust plugins.
          # Tracker integration only needs lightningd, lightning-cli, and the
          # C/Python plugins. Restricting the targets also avoids lowdown's
          # macOS sandbox and unrelated cln-rpc Rust tests.
          cln-base = pkgs.callPackage "${cln-source}/nix/pkgs/default.nix" {
            self = { lastModifiedDate = "20260825000000"; };
            config.packages.rust = throw "Rust CLN plugins are intentionally excluded from tracker integration";
          };
          cln-integration = cln-base.overrideAttrs (old: {
            # GitHub archive inputs do not expand gitlinks. Reconstitute CLN's
            # four submodules from their exact gitlink commits before applying
            # CLN's own platform patches.
            postPatch = ''
              rm -rf external/gheap external/jsmn external/libbacktrace external/libwally-core
              cp -R ${cln-gheap} external/gheap
              cp -R ${cln-jsmn} external/jsmn
              cp -R ${cln-libbacktrace} external/libbacktrace
              cp -R ${cln-libwally} external/libwally-core
              chmod -R u+w external/libwally-core
              rm -rf external/libwally-core/src/secp256k1
              cp -R ${cln-secp256k1} external/libwally-core/src/secp256k1
              chmod -R u+w external/gheap external/jsmn external/libbacktrace external/libwally-core
            '' + (old.postPatch or "") + ''
              substituteInPlace configure \
                --replace-fail \
                  "if ! check_command 'lowdown' lowdown; then" \
                  "if ! command -v lowdown >/dev/null 2>&1; then"
            '';
            buildFlags = (old.buildFlags or [ ]) ++ [ "all-programs" ];
            nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [
              pkgs.openssl
              (pkgs.writeShellScriptBin "gsed" ''
                exec ${pkgs.gnused}/bin/sed "$@"
              '')
            ];
            installTargets = [ "install-program" ];
            postInstall = "";
          });

          integration-test = pkgs.writeShellApplication {
            name = "tracker-integration-test";
            runtimeInputs = [
              cln-integration
              pkgs.bitcoind
              pkgs.coreutils
              pkgs.gnugrep
              pkgs.jq
              tracker
            ];
            text = builtins.readFile ./tests/integration-smoke.sh;
          };
        in {
          inherit tracker cln-source cln-integration integration-test;
        };
    in {
      packages = forAllSystems (system:
        let p = perSystem system;
        in {
          default = p.tracker;
          inherit (p) cln-integration integration-test;
        });

      apps = forAllSystems (system:
        let p = perSystem system;
        in {
          default = {
            type = "app";
            program = "${p.tracker}/bin/cln-tracker";
            meta.description = "Track checksummed Bitcoin descriptors through CLN bwatch";
          };
          integration-test = {
            type = "app";
            program = "${p.integration-test}/bin/tracker-integration-test";
            meta.description = "Run Tracker lifecycle tests with pinned CLN and Bitcoin regtest daemons";
          };
        });

      devShells = forAllSystems (system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          p = perSystem system;
        in {
          default = pkgs.mkShell {
            packages = with pkgs; [
              cargo
              clippy
              pkg-config
              rustc
              rustfmt
            ];
          };

          integration = pkgs.mkShell {
            packages = with pkgs; [
              cargo
              clippy
              p.cln-integration
              p.integration-test
              pkg-config
              rustc
              rustfmt
            ];
            CLN_BWATCH_COMMIT = "c902a7d4a11204fd78b754339f091a2c67f7715f";
            CLN_BWATCH_SOURCE = p.cln-source;
          };
        });
    };
}
