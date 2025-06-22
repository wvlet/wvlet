# Python SDK Test Coverage Analysis

## Quick Tests (python-test.yml) Coverage

The quick test suite covers:

### Unit Tests
- Native library loading and error handling
- Basic query compilation
- Error handling for invalid queries
- Platform detection and library path resolution

### Code Quality
- **Type checking**: `mypy` ensures type safety across the codebase
- **Linting**: `ruff` checks for code style and common issues
- **Import verification**: Tests ensure the package can be imported correctly

## Full Tests (python-publish.yml) Coverage

The full test suite additionally covers:

### Cross-Platform Compatibility
- **Linux x86_64**: Ubuntu latest
- **Linux ARM64**: Ubuntu 24.04 ARM
- **macOS ARM64**: macOS latest
- **Python versions**: 3.9, 3.11, 3.13

### Wheel Building and Distribution
- Platform-specific wheel creation
- Wheel installation verification
- Import testing on each platform

## When to Use Full Tests

Use the `test-wheels` label when:
1. Changing native library loading code
2. Modifying platform detection logic
3. Updating build/packaging configuration
4. Adding new platform support
5. Before releasing a new version

## Test Coverage Adequacy

The quick tests provide sufficient coverage for:
- ✅ Pure Python code changes
- ✅ Documentation updates
- ✅ Type hint modifications
- ✅ Code style improvements
- ✅ Basic functionality verification

The quick tests are NOT sufficient for:
- ❌ Native library interface changes
- ❌ Platform-specific code modifications
- ❌ Packaging/distribution changes
- ❌ Cross-Python version compatibility issues