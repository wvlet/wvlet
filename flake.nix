{
  description = "Wvlet Scala Native build environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
        };

        # Common build dependencies for Scala Native
        buildDeps = with pkgs; [
          # LLVM toolchain
          llvmPackages.clang
          llvmPackages.lld
          llvmPackages.llvm
          # Required native libraries
          boehmgc
          openssl
          zlib
          # Build tools
          pkg-config
        ];

        # Development dependencies (includes JDK and SBT)
        devDeps = with pkgs; [
          jdk21
          sbt
          git
        ];

        # Common shell hook for setting up library paths
        setupHook = ''
          # Set library paths for Scala Native
          export LIBRARY_PATH="${pkgs.boehmgc}/lib:${pkgs.openssl.out}/lib:${pkgs.zlib}/lib''${LIBRARY_PATH:+:$LIBRARY_PATH}"
          export C_INCLUDE_PATH="${pkgs.boehmgc.dev}/include:${pkgs.openssl.dev}/include:${pkgs.zlib.dev}/include''${C_INCLUDE_PATH:+:$C_INCLUDE_PATH}"

          ${pkgs.lib.optionalString pkgs.stdenv.isDarwin ''
            export MACOSX_DEPLOYMENT_TARGET="${pkgs.stdenv.hostPlatform.darwinMinVersion}"
          ''}
        '';

      in {
        # Development shell with all dependencies
        devShells.default = pkgs.mkShell {
          name = "wvlet-dev";
          nativeBuildInputs = devDeps ++ buildDeps;

          shellHook = ''
            echo "Wvlet Scala Native development environment"
            echo ""
            echo "Build dependencies:"
            echo "  clang:   $(clang --version | head -1)"
            echo "  lld:     $(ld.lld --version | head -1)"
            echo "  gc:      ${pkgs.boehmgc}"
            echo "  openssl: ${pkgs.openssl}"
            echo "  zlib:    ${pkgs.zlib}"
            echo ""

            ${setupHook}

            echo "Environment:"
            echo "  LIBRARY_PATH=$LIBRARY_PATH"
            echo "  C_INCLUDE_PATH=$C_INCLUDE_PATH"
            ${pkgs.lib.optionalString pkgs.stdenv.isDarwin ''
              echo "  MACOSX_DEPLOYMENT_TARGET=$MACOSX_DEPLOYMENT_TARGET"
            ''}
            echo ""
            echo "Run: ./sbt wvcLib/nativeLink"
          '';
        };

        # CI shell - dependencies for GitHub Actions (includes sbt for cross-platform consistency)
        devShells.ci = pkgs.mkShell {
          name = "wvlet-ci";
          nativeBuildInputs = buildDeps ++ [ pkgs.sbt pkgs.gnumake ];

          shellHook = ''
            ${setupHook}

            # Export paths for use in CI scripts
            echo "LIBRARY_PATH=$LIBRARY_PATH" >> $GITHUB_ENV 2>/dev/null || true
            echo "C_INCLUDE_PATH=$C_INCLUDE_PATH" >> $GITHUB_ENV 2>/dev/null || true
            ${pkgs.lib.optionalString pkgs.stdenv.isDarwin ''
              echo "MACOSX_DEPLOYMENT_TARGET=$MACOSX_DEPLOYMENT_TARGET" >> $GITHUB_ENV 2>/dev/null || true
            ''}
          '';
        };
      }
    );
}
