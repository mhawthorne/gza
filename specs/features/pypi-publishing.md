# PyPI Publishing

## Overview

Publish gza as a public Python package on PyPI, making it installable via `pip install gza` or `uv pip install gza`.

## Goals

- Make gza easily installable without cloning the repository
- Enable version management and upgrade paths
- Establish automated release workflow

## Current State

The project already has:
- Modern `pyproject.toml` with hatchling build backend
- Proper src-layout (`src/gza/`)
- CLI entry point defined (`gza = gza.cli:main`)
- Minimal runtime dependencies (only `pyyaml>=6.0`)

## Required Changes

### 1. Update pyproject.toml Metadata

Add required and recommended fields:

```toml
[project]
name = "gza"
version = "0.1.0"
description = "A coding AI agent runner for Claude Code"
readme = "README.md"
license = "MIT"
requires-python = ">=3.11"
authors = [
    { name = "Your Name", email = "your@email.com" }
]
keywords = ["claude", "ai", "agent", "runner", "cli", "automation"]
classifiers = [
    "Development Status :: 3 - Alpha",
    "Environment :: Console",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "Topic :: Software Development",
    "Topic :: Software Development :: Libraries",
    "Typing :: Typed",
]
dependencies = [
    "pyyaml>=6.0",
]

[project.urls]
Homepage = "https://github.com/OWNER/gza"
Repository = "https://github.com/OWNER/gza"
Issues = "https://github.com/OWNER/gza/issues"
```

### 2. Add LICENSE File

Create `LICENSE` in project root with MIT license (or preferred license):

```
MIT License

Copyright (c) 2024 Your Name

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### 3. Add Build Dependencies

Update `pyproject.toml` dev dependencies:

```toml
[project.optional-dependencies]
dev = [
    "pre-commit>=3.0",
    "pytest>=8.0",
    "build>=1.0",
    "twine>=5.0",
]
```

### 4. Create GitHub Actions Workflow

Create `.github/workflows/publish.yml`:

```yaml
name: Publish to PyPI

on:
  release:
    types: [published]

jobs:
  publish:
    runs-on: ubuntu-latest
    environment: pypi
    permissions:
      id-token: write  # Required for trusted publishing
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install build tools
        run: pip install build

      - name: Build package
        run: python -m build

      - name: Publish to PyPI
        uses: pypa/gh-action-pypi-publish@release/v1
```

### 5. Package Name Availability

Check if "gza" is available on PyPI: https://pypi.org/project/gza/

If taken, consider alternatives:
- `gza-agent`
- `gza-runner`
- `gza-ai`
- `claude-gza`

## Publishing Process

### Initial Setup (One-time)

1. Create PyPI account at https://pypi.org/account/register/
2. Create TestPyPI account at https://test.pypi.org/account/register/
3. Configure trusted publishing on PyPI:
   - Go to: https://pypi.org/manage/account/publishing/
   - Add new pending publisher with GitHub repository details
4. Add `pypi` environment in GitHub repository settings

### Manual Release Process

```bash
# 1. Update version in pyproject.toml
# 2. Commit version bump
git add pyproject.toml
git commit -m "Bump version to X.Y.Z"
git push

# 3. Create and push tag
git tag vX.Y.Z
git push origin vX.Y.Z

# 4. Create GitHub release (triggers automated publish)
gh release create vX.Y.Z --title "vX.Y.Z" --notes "Release notes here"
```

### Testing on TestPyPI First

```bash
# Build locally
python -m build

# Upload to TestPyPI
python -m twine upload --repository testpypi dist/*

# Test installation
pip install --index-url https://test.pypi.org/simple/ gza
```

## Version Management

### Automated Versioning with hatch-vcs

Use `hatch-vcs` to derive versions automatically from git tags. This eliminates manual version management and enables snapshot releases.

**How it works:**
- Tag a commit with `v0.1.0` → that commit builds as version `0.1.0`
- Commits after the tag → build as `0.1.0.dev1+gABCDEF` (dev release with commit hash)
- No manual version editing in `pyproject.toml`

**Configuration in pyproject.toml:**

```toml
[build-system]
requires = ["hatchling", "hatch-vcs"]
build-backend = "hatchling.build"

[project]
dynamic = ["version"]

[tool.hatch.version]
source = "vcs"

[tool.hatch.build.hooks.vcs]
version-file = "src/gza/_version.py"
```

**Workflow:**
- For stable releases: create a git tag (`git tag v0.2.0`) and publish
- For snapshot releases: CI can publish on every commit (or weekly) with auto-generated dev versions
- Dev versions sort before the next stable release, so `pip install gza` always gets the latest stable

### Semantic Versioning

Follow semantic versioning for tagged releases:
- `0.1.0` → `0.1.1`: Bug fixes
- `0.1.0` → `0.2.0`: New features, backwards compatible
- `0.1.0` → `1.0.0`: Breaking changes or stable release

## Post-Publish Verification

After each release:

```bash
# Verify package is available
pip index versions gza

# Test fresh install
pip install gza==X.Y.Z

# Verify CLI works
gza --help
```

## Tasks

```yaml
group: pypi-publishing
spec: specs/pypi-publishing.md

tasks:
  - prompt: |
      Update pyproject.toml with PyPI metadata.
      Add: readme, license, authors, keywords, classifiers, project.urls.
      Add build and twine to dev dependencies.
    type: implement

  - prompt: |
      Create LICENSE file with MIT license in project root.
    type: implement

  - prompt: |
      Create GitHub Actions workflow for automated PyPI publishing.
      File: .github/workflows/publish.yml
      Use trusted publishing (OIDC) instead of API tokens.
    type: implement
    depends_on: 1

  - prompt: |
      Test build locally: run python -m build and verify dist/ contents.
      Ensure wheel and sdist are created correctly.
    type: task
    depends_on: 2

  - prompt: |
      Upload to TestPyPI and verify installation works.
      Test: pip install --index-url https://test.pypi.org/simple/ gza
    type: task
    depends_on: 4

  - prompt: |
      Set up hatch-vcs for automated git-based versioning.
      1. Add hatch-vcs to build-system.requires
      2. Add dynamic = ["version"] to [project] and remove static version
      3. Add [tool.hatch.version] with source = "vcs"
      4. Add [tool.hatch.build.hooks.vcs] to generate src/gza/_version.py
      5. Create initial git tag (v0.1.0) if none exists
      6. Verify: python -m build should produce correct version in wheel filename
    type: implement
    depends_on: 1
```

## Design Decisions

1. **Trusted Publishing**: Use PyPI's OIDC trusted publishing instead of API tokens. More secure, no secret rotation needed.

2. **Release-triggered publishing**: Only publish on GitHub release creation, not on every tag or push. Gives explicit control over releases.

3. **No CHANGELOG automation**: Keep changelog management manual. Automated changelog generators often produce noise.

4. **Minimal classifiers**: Include only accurate, useful classifiers. Don't pad with every possible option.
