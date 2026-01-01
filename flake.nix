{
  description = "Wvlet Scala Native cross-compilation environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    let
      # Supported build host systems
      supportedSystems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];

      # Import target configurations
      targets = import ./nix/targets.nix;
    in
    flake-utils.lib.eachSystem supportedSystems (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          # Allow unfree packages if needed (e.g., for some native tools)
          config.allowUnfree = true;
        };

        # Import the package builder
        mkWvcLib = import ./nix/mkWvcLib.nix {
          inherit pkgs system;
          inherit (pkgs) lib stdenv;
        };

        # Common development dependencies (run on host)
        commonDevInputs = with pkgs; [
          # JVM for SBT
          jdk21
          # Build tools
          sbt
          git  # Use nixpkgs git instead of Apple Git (avoids FamilyDisplayName warning)
          # Native toolchain
          llvmPackages.clang
          llvmPackages.lld
          llvmPackages.llvm
          # Required libraries for native build
          boehmgc
          openssl
          pkg-config
        ];

        # Create a dev shell for a specific cross-compilation target
        mkCrossDevShell = { targetName, targetConfig }:
          let
            crossPkgs =
              if targetConfig.crossSystem == null then
                pkgs  # Native build
              else
                import nixpkgs {
                  inherit system;
                  crossSystem = targetConfig.crossSystem;
                };

            # Get cross-compiled dependencies
            crossBoehmgc = crossPkgs.boehmgc;
            crossOpenssl = crossPkgs.openssl;

            # Determine the correct clang/lld for cross-compilation
            crossClang =
              if targetConfig.crossSystem == null then
                "${pkgs.llvmPackages.clang}/bin/clang"
              else
                "${crossPkgs.stdenv.cc}/bin/${targetConfig.llvmTriple}-cc";

            crossClangpp =
              if targetConfig.crossSystem == null then
                "${pkgs.llvmPackages.clang}/bin/clang++"
              else
                "${crossPkgs.stdenv.cc}/bin/${targetConfig.llvmTriple}-c++";

            crossLld =
              if targetConfig.crossSystem == null then
                "${pkgs.llvmPackages.lld}/bin/ld.lld"
              else if targetConfig.useLd64 or false then
                "${pkgs.llvmPackages.lld}/bin/ld64.lld"
              else
                "${pkgs.llvmPackages.lld}/bin/ld.lld";

          in pkgs.mkShell {
            name = "wvlet-cross-${targetName}";

            nativeBuildInputs = commonDevInputs ++ (
              if targetConfig.crossSystem != null then
                [ crossPkgs.stdenv.cc ]
              else
                []
            );

            buildInputs =
              if targetConfig.crossSystem == null then
                [ pkgs.boehmgc pkgs.openssl ]
              else
                [ crossBoehmgc crossOpenssl ];

            shellHook = ''
              echo "Wvlet cross-compilation shell for ${targetName}"
              echo "Target triple: ${targetConfig.llvmTriple}"

              # Scala Native environment variables
              export SCALANATIVE_CLANG="${crossClang}"
              export SCALANATIVE_CLANGPP="${crossClangpp}"
              export SCALANATIVE_LLD="${crossLld}"
              export SCALANATIVE_TARGET_TRIPLE="${targetConfig.llvmTriple}"

              ${if targetConfig.crossSystem != null then ''
                export SCALANATIVE_SYSROOT="${crossPkgs.stdenv.cc.libc}"
                export CROSS_GC_INCLUDE="${crossBoehmgc.dev}/include"
                export CROSS_GC_LIB="${crossBoehmgc}/lib"
                export CROSS_OPENSSL_INCLUDE="${crossOpenssl.dev}/include"
                export CROSS_OPENSSL_LIB="${crossOpenssl.out}/lib"
                # Set library paths for cross-compilation
                export LIBRARY_PATH="${crossBoehmgc}/lib:${crossOpenssl.out}/lib:${crossPkgs.zlib}/lib''${LIBRARY_PATH:+:$LIBRARY_PATH}"
                export C_INCLUDE_PATH="${crossBoehmgc.dev}/include:${crossOpenssl.dev}/include:${crossPkgs.zlib.dev}/include''${C_INCLUDE_PATH:+:$C_INCLUDE_PATH}"
              '' else ''
                # Set library paths for native build - prioritize Nix packages over Homebrew
                export LIBRARY_PATH="${pkgs.boehmgc}/lib:${pkgs.openssl.out}/lib:${pkgs.zlib}/lib''${LIBRARY_PATH:+:$LIBRARY_PATH}"
                export C_INCLUDE_PATH="${pkgs.boehmgc.dev}/include:${pkgs.openssl.dev}/include:${pkgs.zlib.dev}/include''${C_INCLUDE_PATH:+:$C_INCLUDE_PATH}"
              ''}

              echo ""
              echo "Environment variables set:"
              echo "  SCALANATIVE_CLANG=$SCALANATIVE_CLANG"
              echo "  SCALANATIVE_CLANGPP=$SCALANATIVE_CLANGPP"
              echo "  SCALANATIVE_LLD=$SCALANATIVE_LLD"
              echo "  SCALANATIVE_TARGET_TRIPLE=$SCALANATIVE_TARGET_TRIPLE"
              echo "  LIBRARY_PATH=$LIBRARY_PATH"
              ${if targetConfig.crossSystem != null then ''
                echo "  SCALANATIVE_SYSROOT=$SCALANATIVE_SYSROOT"
              '' else ""}
              echo ""
              echo "Run: ./sbt wvcLib/nativeLink"
            '';
          };

        # Filter targets based on current host system
        availableTargets = pkgs.lib.filterAttrs (name: config:
          # Check if this target can be built from the current host
          builtins.elem system config.buildHosts
        ) targets;

      in {
        # Dev shells for each target
        devShells = {
          # Default shell for native development
          default = pkgs.mkShell {
            name = "wvlet-dev";
            nativeBuildInputs = commonDevInputs;
            buildInputs = [ pkgs.boehmgc pkgs.openssl pkgs.zlib ];

            shellHook = ''
              echo "Wvlet development shell"
              echo "Available cross-compilation targets: ${builtins.concatStringsSep ", " (builtins.attrNames availableTargets)}"
              echo ""

              # Set library paths - prioritize Nix packages over Homebrew
              export LIBRARY_PATH="${pkgs.boehmgc}/lib:${pkgs.openssl.out}/lib:${pkgs.zlib}/lib''${LIBRARY_PATH:+:$LIBRARY_PATH}"
              export C_INCLUDE_PATH="${pkgs.boehmgc.dev}/include:${pkgs.openssl.dev}/include:${pkgs.zlib.dev}/include''${C_INCLUDE_PATH:+:$C_INCLUDE_PATH}"

              ${pkgs.lib.optionalString pkgs.stdenv.isDarwin ''
                # Set macOS deployment target to a compatible version
                export MACOSX_DEPLOYMENT_TARGET="${pkgs.stdenv.hostPlatform.darwinMinVersion}"
              ''}

              echo "LIBRARY_PATH=$LIBRARY_PATH"
              echo "MACOSX_DEPLOYMENT_TARGET=$MACOSX_DEPLOYMENT_TARGET"
              echo ""
              echo "Enter a cross-compilation shell with:"
              echo "  nix develop .#<target-name>"
              echo ""
            '';
          };
        } // pkgs.lib.mapAttrs (name: config:
          mkCrossDevShell { targetName = name; targetConfig = config; }
        ) availableTargets;

        # Packages (to be implemented - for now just expose shell)
        packages = {
          # TODO: Add actual package derivations
        };
      }
    );
}
