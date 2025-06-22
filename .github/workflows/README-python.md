# Python SDK CI/CD Workflows

## Overview

The Python SDK has two main workflows:

### 1. `python-test.yml` - Quick Tests (Every PR)
- **Runs on**: Every PR and push to main that touches Python SDK
- **Duration**: ~2 minutes
- **Tests**: 
  - Unit tests on Ubuntu with Python 3.11
- **Purpose**: Fast feedback for developers

### 2. `python-publish.yml` - Full Build and Cross-Platform Tests
- **Runs on**:
  - Version tags (`v*`)
  - PRs labeled with `test-wheels`
  - Weekly schedule (Sundays at 00:00 UTC)
  - Manual workflow dispatch
- **Duration**: ~15-20 minutes
- **Tests**: 
  - Full matrix: 3 OS × 3 Python versions = 9 jobs
  - Builds platform-specific wheels
  - Tests on Linux x86_64, Linux ARM64, and macOS ARM64
  - Python versions: 3.9, 3.11, 3.13

## When to Use Full Tests

Add the `test-wheels` label to your PR when:
- Making changes to native library loading
- Modifying platform-specific code
- Changing the build/packaging configuration
- Before releasing a new version

## Workflow Triggers

| Workflow | PR (every) | PR (labeled) | Tag | Schedule | Manual |
|----------|------------|--------------|-----|----------|---------|
| python-test.yml | ✓ | - | - | - | - |
| python-publish.yml | - | ✓ (test-wheels) | ✓ | ✓ (weekly) | ✓ |

## Publishing to PyPI

Publishing happens automatically when:
1. A version tag is pushed (`v*`)
2. Build succeeds
3. Tests pass (or are skipped for non-critical paths)

The publish job uses PyPI trusted publishing with OIDC.