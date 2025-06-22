# Publishing TypeScript SDK to NPM

This document describes how the TypeScript SDK is published to NPM.

## Automated Publishing

### Release Versions

When a new release is created on GitHub (with a tag like `v0.1.0`), the NPM package is automatically published:

1. Create a GitHub release with a version tag (e.g., `v0.1.0`)
2. The `typescript-npm-publish.yml` workflow automatically:
   - Builds the Scala.js SDK
   - Runs tests
   - Updates the package version
   - Publishes to NPM with provenance

## Manual Publishing

The release workflow can also be triggered manually:

1. Go to Actions → "TypeScript NPM Publish" workflow
2. Click "Run workflow"
3. Enter the version to publish
4. The workflow will build and publish the specified version

## NPM Token Setup

The workflows require an NPM token to be set up as a GitHub secret:

1. Generate an NPM access token at https://www.npmjs.com/
2. Add it as `NPM_TOKEN` in the repository secrets

## Version Management

- The base version is maintained in `package.json`
- Release versions are set from the Git tag
- All publishing includes NPM provenance for security

## Testing Before Release

Before creating a release:

1. Ensure all tests pass: `npm test`
2. Build the package: `npm run build`
3. Test the package locally: `npm pack`
4. Verify the package contents

## Package Contents

The published package includes:
- `dist/` - Compiled TypeScript files
- `lib/` - Scala.js compiled output
- `README.md` - Package documentation
- Type definitions