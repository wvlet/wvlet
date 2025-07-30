---
sidebar_label: VS Code Extension
sidebar_position: 8
---

# VS Code Extension for Wvlet

The Wvlet extension for Visual Studio Code provides comprehensive language support for `.wv` files, making it easier to write and work with Wvlet queries.

## Installation

### From VS Code Marketplace (Recommended)

The easiest way to install the extension is directly from the VS Code Marketplace:

1. Open VS Code
2. Go to the Extensions view (`Ctrl+Shift+X` on Windows/Linux, `Cmd+Shift+X` on macOS)
3. Search for "Wvlet"
4. Click "Install"

Alternatively, install directly from the [VS Code Marketplace page](https://marketplace.visualstudio.com/items?itemName=wvlet.wvlet).

### From VSIX File

For manual installation or testing pre-release versions:

1. Download the `.vsix` file from [GitHub Releases](https://github.com/wvlet/wvlet/releases)
2. In VS Code, go to the Extensions view
3. Click the "..." menu and select "Install from VSIX..."
4. Select the downloaded `.vsix` file

## Features

### Syntax Highlighting

The extension provides complete syntax highlighting for Wvlet queries, including:

- **Keywords**: All Wvlet keywords like `from`, `where`, `select`, `group by`, `transform`, etc.
- **Data Types**: Highlighting for types like `int`, `string`, `date`, `decimal`, etc.
- **Operators**: All operators including comparison, arithmetic, and special operators
- **Comments**: Both single-line (`--`) and multi-line (`---`) comments
- **Strings**: Support for single quotes, double quotes, backticks, and triple-quoted strings
- **String Interpolation**: Highlighting for `${...}` expressions within strings
- **Numbers**: Integers, floating-point numbers, and hexadecimal literals

### Language Configuration

- **Bracket Matching**: Automatic matching for `{}`, `[]`, `()`, and `${}`
- **Auto-closing Pairs**: Brackets and quotes automatically close when typed
- **Comment Toggling**: Use keyboard shortcuts to toggle comments on selected lines
- **Word Pattern Recognition**: Smart double-click selection for Wvlet identifiers

## Example

Here's how a Wvlet query looks with syntax highlighting:

```sql
-- Starting with table scan
from lineitem
-- Apply filters
where l_shipdate >= '1994-01-01'
  and l_shipdate < '1995-01-01'
-- Group by ship mode
group by l_shipmode
-- Calculate aggregations
agg _.count as cnt,
    l_quantity.sum.round(2) as total_qty
-- Order results
order by cnt desc
```

## Version Management

The extension follows a versioning scheme independent of the main Wvlet project:

- **Stable Releases**: Version numbers ending in `.0` (e.g., `2025.1.0`)
- **Pre-release Versions**: Version numbers with patch > 0 (e.g., `2025.1.1`)

### Installing Pre-release Versions

To try new features before they're officially released:

1. Go to the extension page in VS Code
2. Click the gear icon next to "Uninstall"
3. Select "Switch to Pre-Release Version"

## Troubleshooting

### Extension Not Working

1. Ensure you have the latest version of VS Code (1.74.0 or higher)
2. Check that the file has a `.wv` extension
3. Try reloading the VS Code window (`Ctrl+Shift+P` / `Cmd+Shift+P` → "Developer: Reload Window")

### Reporting Issues

Report issues or request features on the [Wvlet GitHub repository](https://github.com/wvlet/wvlet/issues).

## Development

The extension is open source and part of the main Wvlet repository. To contribute:

1. Clone the [Wvlet repository](https://github.com/wvlet/wvlet)
2. Navigate to the `vscode-wvlet` directory
3. See the [BUILD.md](https://github.com/wvlet/wvlet/blob/main/vscode-wvlet/BUILD.md) file for development instructions

## Related Links

- [VS Code Marketplace Page](https://marketplace.visualstudio.com/items?itemName=wvlet.wvlet)
- [Extension Source Code](https://github.com/wvlet/wvlet/tree/main/vscode-wvlet)
- [Wvlet Documentation](https://wvlet.org/docs)