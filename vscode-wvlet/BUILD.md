# VS Code Extension for Wvlet

This directory contains the VS Code extension for Wvlet language support.

## Features

- Syntax highlighting for `.wv` files
- Language configuration with bracket matching and auto-closing
- Comment toggling support
- Support for all Wvlet keywords, operators, and syntax

## Installation

### From VSIX File

1. Build the extension: `npm run package`
2. Install in VS Code: Extensions → "..." → "Install from VSIX..." → select `wvlet-X.X.X.vsix`

### Development

1. Install dependencies: `npm install`
2. Make changes to language files
3. Test: Press `F5` in VS Code to launch a new window with the extension
4. Package: `npm run package`

## Files

- `package.json` - Extension manifest and metadata
- `language-configuration.json` - Language configuration (brackets, comments, etc.)
- `syntaxes/wvlet.tmLanguage.json` - TextMate grammar for syntax highlighting
- `test.wv` - Sample Wvlet file for testing

## Building from Monaco Editor

This extension was created by converting the Monaco Editor language definition from:
- `wvlet-ui-main/src/main/scala/wvlet/lang/ui/component/monaco/WvletLanguage.ts`

The conversion process involved:
1. Converting Monaco's IMonarchLanguage to TextMate grammar format
2. Adapting language configuration for VS Code
3. Setting up proper file associations and scope names

## Versioning

The extension uses two version types:

1. **Development version**: `2025.1.0-dev` - For testing and development
2. **Release version**: `2025.1.0` - For stable releases

To build a development version:
1. Ensure `package.json` has `-dev` suffix in version
2. Run `npm run build-vscode-extension`
3. Test locally with the generated VSIX file

For releases, remove the `-dev` suffix before publishing.

## CI/CD

The extension is automatically built on GitHub Actions when changes are made to this directory. The workflow can also publish to the VS Code Marketplace on releases.