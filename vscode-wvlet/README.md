# Wvlet Language Support for VS Code

This extension provides syntax highlighting and language support for the Wvlet query language in Visual Studio Code.

## Features

- **Syntax Highlighting**: Full syntax highlighting for `.wv` files including:
  - Keywords and control structures
  - Data types and type annotations
  - Operators and expressions
  - String literals with interpolation support
  - Numeric literals (integers, floats, hex)
  - Comments (single-line `--` and multi-line `---`)

- **Language Configuration**: 
  - Bracket matching and auto-closing
  - Comment toggling
  - Word pattern recognition

## Supported File Extensions

- `.wv` - Wvlet query files

## Keywords

The extension recognizes Wvlet language keywords including:

### Control Flow
- `if`, `then`, `else`, `end`, `case`, `when`

### Query Operations
- `select`, `from`, `where`, `group`, `having`, `order`, `limit`
- `join`, `on`, `left`, `right`, `full`, `inner`, `cross`
- `transform`, `pivot`, `distinct`, `asc`, `desc`

### Data Operations
- `add`, `prepend`, `exclude`, `rename`, `shift`, `drop`
- `concat`, `dedup`, `intersect`, `except`

### Other Language Features
- `def`, `type`, `model`, `import`, `export`, `package`
- `test`, `should`, `be`, `contain`, `debug`
- `save`, `append`, `delete`, `truncate`

## Data Types

- `boolean`, `int`, `long`, `float`, `double`, `string`, `byte`, `short`, `char`, `void`
- `array`, `map`, `date`, `decimal`, `interval`

## Installation

### From VSIX (Recommended)

1. Download the latest `.vsix` file from the releases
2. Open VS Code
3. Go to Extensions view (`Ctrl+Shift+X`)
4. Click the "..." menu and select "Install from VSIX..."
5. Select the downloaded `.vsix` file

### From Source

1. Clone the repository
2. Navigate to the `vscode-wvlet` directory
3. Install dependencies: `npm install`
4. Package the extension: `npm run package`
5. Install the generated `.vsix` file

## Development

To contribute to this extension:

1. Clone the Wvlet repository
2. Navigate to `vscode-wvlet/`
3. Install dependencies: `npm install`
4. Make your changes
5. Test by pressing `F5` to launch a new VS Code window with the extension loaded
6. Package with `npm run package`

## License

Licensed under the Apache License, Version 2.0. See the main Wvlet repository for details.

## Links

- [Wvlet Project](https://wvlet.org)
- [GitHub Repository](https://github.com/wvlet/wvlet)
- [Documentation](https://wvlet.org/docs)