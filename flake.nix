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

        # Pin libduckdb to the same version the CI test jobs install (see
        # DUCKDB_VERSION at the repo root). nixpkgs-24.11 ships an older 1.1.3,
        # so we override `pkgs.duckdb` with a small derivation that fetches the
        # prebuilt artifact straight from DuckDB's GitHub release — same source
        # `.github/workflows/test.yml` uses. Bumping DuckDB is a two-step edit:
        # update DUCKDB_VERSION, then refresh the per-platform sha256s below.
        duckdbVersion = builtins.replaceStrings ["\n"] [""]
          (builtins.readFile ./DUCKDB_VERSION);

        duckdbArtifacts = {
          "x86_64-linux"   = { name = "libduckdb-linux-amd64.zip";
                               sha256 = "4711438f0fdb04f0441803409bec5430b763d4f2ac3482c1f97cfa6b5ecb4c15"; };
          "aarch64-linux"  = { name = "libduckdb-linux-arm64.zip";
                               sha256 = "b4acbd9d871c99788c303af982f2e39325f28fd415f9a927d0a73e206f09e75d"; };
          "x86_64-darwin"  = { name = "libduckdb-osx-universal.zip";
                               sha256 = "524f3537330a1b747556a0c98b62a46865a3f48c7ead2b2035c62f1ad3e5ca8b"; };
          "aarch64-darwin" = { name = "libduckdb-osx-universal.zip";
                               sha256 = "524f3537330a1b747556a0c98b62a46865a3f48c7ead2b2035c62f1ad3e5ca8b"; };
        };

        libduckdb =
          let artifact = duckdbArtifacts.${pkgs.stdenv.hostPlatform.system};
          in pkgs.stdenv.mkDerivation {
            pname = "libduckdb";
            version = duckdbVersion;

            src = pkgs.fetchurl {
              url = "https://github.com/duckdb/duckdb/releases/download/v${duckdbVersion}/${artifact.name}";
              sha256 = artifact.sha256;
            };

            nativeBuildInputs = [ pkgs.unzip ];
            outputs = [ "out" "dev" "lib" ];

            unpackPhase = "unzip $src";

            installPhase = ''
              mkdir -p $lib/lib $dev/include $out
              cp libduckdb.so $lib/lib/ 2>/dev/null || true
              cp libduckdb.dylib $lib/lib/ 2>/dev/null || true
              cp duckdb.h $dev/include/
              [ -f duckdb.hpp ] && cp duckdb.hpp $dev/include/ || true
            '';

            # The shared lib is already a release artifact; stripping it can break
            # the Boehm GC pointer scan and is unnecessary here.
            dontStrip = true;
          };

        # Common build dependencies for Scala Native
        buildDeps = [
          # LLVM toolchain
          pkgs.llvmPackages.clang
          pkgs.llvmPackages.lld
          pkgs.llvmPackages.llvm
          # Required native libraries
          pkgs.boehmgc
          libduckdb        # see derivation above — pinned to DUCKDB_VERSION
          pkgs.openssl
          pkgs.zlib
          # Build tools
          pkgs.pkg-config
        ];

        # Development dependencies (includes JDK and SBT)
        devDeps = with pkgs; [
          jdk21
          sbt
          git
        ];

        # Common shell hook for setting up library paths
        setupHook = ''
          # Set library paths for Scala Native. libduckdb is multi-output: libraries live in
          # `.lib` and headers in `.dev`.
          export LIBRARY_PATH="${pkgs.boehmgc}/lib:${libduckdb.lib}/lib:${pkgs.openssl.out}/lib:${pkgs.zlib}/lib''${LIBRARY_PATH:+:$LIBRARY_PATH}"
          export C_INCLUDE_PATH="${pkgs.boehmgc.dev}/include:${libduckdb.dev}/include:${pkgs.openssl.dev}/include:${pkgs.zlib.dev}/include''${C_INCLUDE_PATH:+:$C_INCLUDE_PATH}"

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
            echo "  duckdb:  ${libduckdb.lib} (v${duckdbVersion})"
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
