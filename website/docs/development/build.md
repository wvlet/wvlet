# Building Wvlet


## Building from Source 

To build Wvlet, you will need at least JDK17 or later. To test Trino connector, JDK24 or later is required (as of December 2024).  

```bash
# Clone the source code repository
git clone git@github.com:wvlet/wvlet.git
cd wvlet
./sbt
## This will install wv command to your ~/local/bin
sbt:wvlet> cli/packInstall
```

You can find the wvlet command in `~/local/bin/wv`. For convenience, you can set `PATH` environment variable like this:
```bash title='~/.bashenv'
export PATH=$HOME/local/bin:$PATH
```

:::info
Mac users can install wvlet with Homebrew: [`brew install wvlet/wvlet/wvlet`](../usage/install.md)
:::


## Building Documentation 

Wvlet Documentation https://wvlet.org/wvlet is built with Docusaurus and GitHub Pages. To edit the documentation, you can start a local server to preview the documentation changes: 

```bash
cd website
```

Then start a documentation server at localhost:3000:
```bash
pnpm --filter website run start
```

(The repository uses [pnpm](https://pnpm.io) workspaces. Install pnpm with
`corepack enable` or `npm install -g pnpm`. Run `pnpm install` once at the
repo root before the first build.)

The server will be reloaded automatically when you update .md files.


`website/docs/` directory contains the markdown files for the documentation. Once your change is merged to the main branch, GitHub Action will update the public website automatically.


## Building Native Libraries


Wvlet can be compiled to a native library using Scala Native, which compiles Scala code to a native binary for the target OS and CPU architecture.

:::info
To build native libraries, you need to install `clang`, `llvm`, `libstdc++-12-dev`, and `libgc` (Boehm GC) on your system. See also [Scala Native Setup](https://scala-native.org/en/latest/user/setup.html). 
:::

### DuckDB Dependency

Native binaries link [libduckdb](https://duckdb.org/docs/installation/) dynamically, so both `duckdb.h` and the libduckdb shared library must be visible to the C compiler and linker. Installing DuckDB with a package manager (e.g. `brew install duckdb` on macOS) is usually enough. If they are installed in a non-standard location, point the standard clang environment variables at them before building:

```bash
# Directory containing duckdb.h
export CPATH=/path/to/duckdb/include
# Directory containing libduckdb.dylib (macOS) or libduckdb.so (Linux)
export LIBRARY_PATH=/path/to/duckdb/lib
```

At runtime, the built binaries look up libduckdb in the standard loader locations plus `/opt/homebrew/lib` and `/usr/local/lib`, which are embedded as rpath entries. To run with a libduckdb installed elsewhere, either set `WVLET_NATIVE_LIB_PATH=/path/to/duckdb/lib` at **build time** (the directory is embedded as an additional `-L`/rpath entry), or set `DYLD_LIBRARY_PATH` (macOS) / `LD_LIBRARY_PATH` (Linux) when launching the binary.

To build libwvlet.so (Linux) or libwvlet.dylib (macOS), run the following command:

```bash
$ ./sbt
sbt:wvlet> wvcLib/nativeLink 
```

The library files will be generated in `wvc-lib/target/scala-(SCALA_VERSION)/` directory.

You can use methods defined in this library from C, C++, Rust, etc. 

Test the Rust binding:
```
$ cd wvc-lib
$ make rust
```

Test compiling Wvlet into SQL:
```
$ make rust ARGS='-q "select 1"'
```

## Building Native Compiler (wvc)

Standalone compiler (wvc) can be built with Scala Native.


```bash
$ ./sbt
sbt:wvlet> wvc/nativeLink
```

```bash
$ ./wvc/target/scala-3.3.4/wvc -q "select 1"
-- wvlet version=2024.9.12, src=01JD5Q1CSBH59S686VS2RGWZE3.wv:1
select 1
```
