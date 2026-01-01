# Package derivation builder for wvcLib
#
# This module provides functions to build wvcLib as a Nix package.
# Note: Building Scala/SBT projects in Nix requires special handling
# for dependency fetching. For now, this provides the structure for
# future implementation.

{ pkgs, lib, stdenv, system }:

let
  targets = import ./targets.nix;

  # Create a derivation for building wvcLib for a specific target
  mkWvcLibPackage = { targetName, targetConfig, src }:
    let
      crossPkgs =
        if targetConfig.crossSystem == null then
          pkgs
        else
          import <nixpkgs> {
            inherit system;
            crossSystem = targetConfig.crossSystem;
          };

      crossBoehmgc = crossPkgs.boehmgc;
      crossOpenssl = crossPkgs.openssl;

    in stdenv.mkDerivation {
      pname = "wvcLib-${targetName}";
      version = "0.1.0";  # TODO: Extract from build.sbt

      inherit src;

      nativeBuildInputs = with pkgs; [
        jdk21
        sbt
        llvmPackages.clang
        llvmPackages.lld
      ] ++ lib.optionals (targetConfig.crossSystem != null) [
        crossPkgs.stdenv.cc
      ];

      buildInputs = [
        crossBoehmgc
        crossOpenssl
      ];

      # SBT needs a writable home directory (use relative path for sandboxed isolation)
      HOME = "./.sbt-home";

      # Environment variables for Scala Native
      SCALANATIVE_TARGET_TRIPLE = targetConfig.llvmTriple;

      buildPhase = ''
        mkdir -p $HOME

        # Set up cross-compilation environment
        export SCALANATIVE_CLANG="${if targetConfig.crossSystem == null
          then "${pkgs.llvmPackages.clang}/bin/clang"
          else "${crossPkgs.stdenv.cc}/bin/${targetConfig.llvmTriple}-cc"}"

        export SCALANATIVE_CLANGPP="${if targetConfig.crossSystem == null
          then "${pkgs.llvmPackages.clang}/bin/clang++"
          else "${crossPkgs.stdenv.cc}/bin/${targetConfig.llvmTriple}-c++"}"

        export SCALANATIVE_LLD="${pkgs.llvmPackages.lld}/bin/${if targetConfig.useLd64 or false then "ld64.lld" else "ld.lld"}"

        ${lib.optionalString (targetConfig.crossSystem != null) ''
          export SCALANATIVE_SYSROOT="${crossPkgs.stdenv.cc.libc}"
          export CROSS_GC_INCLUDE="${crossBoehmgc.dev}/include"
          export CROSS_GC_LIB="${crossBoehmgc}/lib"
        ''}

        # Run SBT build
        ./sbt wvcLib/nativeLink
      '';

      installPhase = ''
        mkdir -p $out/lib

        # Copy the built library
        cp $(find wvc-lib/target -type f -name "libwvlet.${targetConfig.libSuffix}") $out/lib/

        # Copy headers if available
        if [ -d wvc-lib/include ]; then
          mkdir -p $out/include
          cp -r wvc-lib/include/* $out/include/
        fi
      '';

      meta = with lib; {
        description = "Wvlet native library for ${targetName}";
        homepage = "https://github.com/wvlet/wvlet";
        license = licenses.asl20;
        platforms = targetConfig.buildHosts;
      };
    };

in {
  inherit mkWvcLibPackage;

  # Convenience function to build all available targets for the current system
  buildAllTargets = src:
    lib.mapAttrs (name: config:
      mkWvcLibPackage {
        targetName = name;
        targetConfig = config;
        inherit src;
      }
    ) (lib.filterAttrs (name: config:
      builtins.elem system config.buildHosts
    ) targets);
}
