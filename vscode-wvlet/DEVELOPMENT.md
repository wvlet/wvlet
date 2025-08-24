# VS Code Extension Development

## Important Version Synchronization

When updating VS Code related dependencies, ensure that:
- `engines.vscode` version in package.json matches the `@types/vscode` version
- Both should be updated together to avoid CI build failures

Example:
```json
{
  "engines": {
    "vscode": "^1.103.0"  // Must match @types/vscode version
  },
  "devDependencies": {
    "@types/vscode": "^1.103.0"  // Must match engines.vscode version
  }
}
```

## Build and Test

```bash
# Package the extension
npm run package
```

## CI Requirements

The CI will verify that `@types/vscode` version is compatible with the declared `engines.vscode` version. If there's a mismatch, the build will fail with an error message indicating the version incompatibility.