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

## Making a Pre-release

To create a pre-release version of the extension:

1. Update the version in `package.json` with a pre-release suffix:
   - For development builds: `2025.1.0-dev`
   - For beta releases: `2025.1.0-beta.1`
   - For release candidates: `2025.1.0-rc.1`

2. Build the pre-release package:
   ```bash
   npm run build-vscode-extension
   ```

3. The VSIX file will be named with the pre-release version (e.g., `wvlet-2025.1.0-dev.vsix`)

4. For testing:
   - Install locally using the VSIX file
   - Or share the VSIX file with beta testers

5. To publish as a pre-release to the marketplace:
   ```bash
   cd vscode-wvlet
   npx vsce publish --pre-release
   ```

Note: Pre-releases allow users to opt-in to test new features before stable release.

## CI/CD

The extension is automatically built on GitHub Actions when changes are made to this directory. The workflow can also publish to the VS Code Marketplace on releases.