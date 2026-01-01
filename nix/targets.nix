# Target platform configurations for Scala Native cross-compilation
#
# Each target defines:
# - llvmTriple: The LLVM target triple for Scala Native
# - crossSystem: nixpkgs cross-compilation system config (null for native)
# - buildHosts: List of systems that can build this target
# - libSuffix: File extension for shared libraries
# - useLd64: Whether to use ld64.lld (macOS) instead of ld.lld

{
  # macOS ARM64 - native build only on macOS ARM
  # Use arm64-apple-darwin to match Nix's clang wrapper expectations
  darwin-arm64 = {
    llvmTriple = "arm64-apple-darwin";
    crossSystem = null;  # Native build only
    buildHosts = [ "aarch64-darwin" ];
    libSuffix = "dylib";
    useLd64 = true;
  };

  # Linux ARM64 - native on ARM, cross from x86_64 Linux or macOS
  # Note: Cross-compilation from macOS requires -Dos.name=linux to prevent
  # macOS-specific linker flags from being passed to the cross-linker.
  linux-arm64 = {
    llvmTriple = "aarch64-unknown-linux-gnu";
    crossSystem = {
      config = "aarch64-unknown-linux-gnu";
      system = "aarch64-linux";
    };
    buildHosts = [ "aarch64-linux" "x86_64-linux" "aarch64-darwin" ];
    libSuffix = "so";
    useLd64 = false;
  };

  # Linux x86_64 - native on x86_64, cross from ARM Linux or macOS
  # Note: Cross-compilation from macOS requires -Dos.name=linux (same as above)
  linux-x64 = {
    llvmTriple = "x86_64-unknown-linux-gnu";
    crossSystem = {
      config = "x86_64-unknown-linux-gnu";
      system = "x86_64-linux";
    };
    buildHosts = [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" ];
    libSuffix = "so";
    useLd64 = false;
  };

  # Windows ARM64 - cross from Linux only
  windows-arm64 = {
    llvmTriple = "aarch64-w64-mingw32";
    crossSystem = {
      config = "aarch64-w64-mingw32";
      libc = "msvcrt";
    };
    buildHosts = [ "x86_64-linux" "aarch64-linux" ];
    libSuffix = "dll";
    useLd64 = false;
    isWindows = true;
  };

  # Windows x86_64 - cross from Linux only (macOS has LLVM CodeViewDebug crash)
  windows-x64 = {
    llvmTriple = "x86_64-w64-mingw32";
    crossSystem = {
      config = "x86_64-w64-mingw32";
      libc = "msvcrt";
    };
    buildHosts = [ "x86_64-linux" "aarch64-linux" ];
    libSuffix = "dll";
    useLd64 = false;
    isWindows = true;
  };
}
