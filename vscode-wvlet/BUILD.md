# VS Code Extension for Wvlet

This directory contains the VS Code extension for Wvlet language support.

## Features

- Syntax highlighting for `.wv` files
- Language configuration with bracket matching and auto-closing
- Comment toggling support
- Support for all Wvlet keywords, operators, and syntax

## Development

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

The extension follows a YYYY.(milestone).(patch) versioning scheme:
- **Stable releases**: YYYY.(milestone).0 (e.g., `2025.1.0`, `2025.2.0`)
- **Pre-release versions**: YYYY.(milestone).(patch > 0) (e.g., `2025.1.1`, `2025.1.2`)

### Pre-release Versions

Pre-release versions use patch numbers greater than 0 and are published for testing new features:

1. Update `package.json` to increment the patch version (e.g., `2025.1.1`, `2025.1.2`)
2. Build the extension: `npm run build-vscode-extension`
3. Publish as pre-release: `vsce publish --pre-release`

Pre-release versions:
- Are marked with a "Pre-Release" badge in the marketplace
- Users can opt-in to install pre-release versions through VS Code settings
- Allow testing new features without affecting stable users

### Stable Releases

Stable releases always use patch version 0:

1. Update `package.json` to a .0 version (e.g., `2025.1.0`, `2025.2.0`)
2. Build: `npm run build-vscode-extension`
3. Publish: `vsce publish`

### Version Examples

- `2025.1.1` - First pre-release for milestone 2025.1
- `2025.1.2` - Second pre-release with bug fixes
- `2025.1.0` - Stable release for milestone 2025.1
- `2025.2.1` - First pre-release for milestone 2025.2
- `2025.2.0` - Stable release for milestone 2025.2

## CI/CD

The extension is automatically built on GitHub Actions when changes are made to this directory.

### Automated Releases

The VS Code extension has its own release cycle independent of Wvlet releases. To publish:

1. Update version in `package.json`:
   - For pre-release: YYYY.(milestone).(patch > 0) e.g., `2025.1.1`
   - For stable release: YYYY.(milestone).0 e.g., `2025.1.0`

2. Push changes to main branch

3. Go to Actions → VS Code Extension → Run workflow

4. Check "Publish to VS Code Marketplace" and run

The workflow will automatically determine the release type based on the version:
- Patch version = 0 → Publishes as stable release
- Patch version > 0 → Publishes as pre-release