# Change Log

All notable changes to the Wvlet extension will be documented in this file.

## [2025.2.0] - 2025-01-29

### Changed
- Extension renamed from "wvlet-language-support" to "wvlet"
- Integrated with npm workspaces for better build management
- Updated CI/CD workflow to support independent release cycle from main Wvlet project
- Improved documentation with clearer installation and build instructions

### Fixed
- Reduced extension package size from 490MB to ~21KB by fixing .vscodeignore
- Fixed test syntax files to use valid Wvlet syntax based on documentation
- Corrected floating-point number regex pattern

## [2025.1.1] - 2025-01-29 (Pre-release)

### Changed
- Updated versioning scheme to YYYY.(milestone).(patch) format
  - Stable releases: patch = 0
  - Pre-releases: patch > 0
- Added automated release type detection in CI/CD workflow
- Simplified GitHub Actions workflow with single publish checkbox
- Updated BUILD.md with detailed pre-release publishing instructions

### Fixed
- Fixed floating-point number regex to match numbers starting with 0 (e.g., `0.5`, `0.123`)

## [2025.1.0] - 2025-01-28

### Added
- Initial release of Wvlet for VS Code
- Complete syntax highlighting for `.wv` files including:
  - All Wvlet keywords (`from`, `select`, `where`, `group`, `by`, etc.)
  - SQL-compatible keywords (`join`, `on`, `left`, `right`, `inner`, etc.)
  - Data processing keywords (`transform`, `pivot`, `agg`, `distinct`, etc.)
  - Testing keywords (`test`, `should`, `be`, `contain`)
  - Model and type keywords (`model`, `def`, `type`, `extends`)
  - Additional keywords (`unnest`, `lateral`, `subscribe`, `watermark`, `incremental`, `insert`, `into`, `create`)
- Language configuration features:
  - Bracket matching for `{}`, `[]`, `()`, and `${}`
  - Auto-closing pairs for brackets and quotes
  - Comment toggling (single-line `--` and multi-line `---`)
- String support:
  - Single quotes, double quotes, and backticks
  - Triple-quoted strings for multi-line text
  - String interpolation with `${...}` syntax
- Number literal highlighting:
  - Integers with optional underscores (e.g., `1_000_000`)
  - Floating-point numbers (e.g., `3.14`, `0.5`)
  - Hexadecimal numbers (e.g., `0xFF`)
- Type annotation support for Wvlet data types
- Integration with npm workspaces for streamlined build process
- VS Code Marketplace publishing support