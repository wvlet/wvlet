# Wvlet Python SDK

Python SDK for compiling Wvlet queries into SQL.

## Installation

```bash
# Install from PyPI (when published)
pip install wvlet

# Install from source
pip install -e "git+https://github.com/wvlet/wvlet/#egg=wvlet&subdirectory=sdks/python"
```

## Usage

### Simple usage

```python
from wvlet import compile

# Compile a Wvlet query to SQL
sql = compile("from users select name, age where age > 18")
print(sql)
```

### Using the compiler class

```python
from wvlet.compiler import WvletCompiler

# Create a compiler instance
compiler = WvletCompiler()

# Compile queries
sql = compiler.compile("from users select name, age")
print(sql)

# Compile for a specific target database
compiler = WvletCompiler(target="trino")
sql = compiler.compile("from users select name, age")
```

## How it works

The Python SDK uses a native library (`libwvlet.so` on Linux, `libwvlet.dylib` on macOS) 
that is bundled with the package. This means you don't need to install the Wvlet CLI 
separately.

If the native library is not available for your platform, the SDK will fall back to 
using the `wvlet` command-line tool if it's installed in your PATH.

## Supported Platforms

Currently supported platforms for the native library:
- Linux x86_64
- Linux aarch64 (ARM64)
- macOS arm64 (Apple Silicon)

Other platforms will use the CLI fallback if available.

## Development

To run tests:

```bash
cd sdks/python
pytest
```
