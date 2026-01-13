# hatch-vcs Setup Complete

## Configuration Changes Made

The following changes have been made to `pyproject.toml` to enable automated git-based versioning:

1. ✅ Added `hatch-vcs` to `build-system.requires`
2. ✅ Changed `version = "0.0.1"` to `dynamic = ["version"]` in `[project]`
3. ✅ Added `[tool.hatch.version]` section with `source = "vcs"`
4. ✅ Added `[tool.hatch.build.hooks.vcs]` section with `version-file = "src/gza/_version.py"`

## Next Steps (Manual - Git Tag Creation)

To complete the setup, you need to create an initial git tag in your repository:

```bash
# Navigate to your repository root
cd /path/to/gza

# Create and push the initial tag
git tag v0.1.0
git push origin v0.1.0
```

## Verification

After creating the tag, verify the setup works:

```bash
# Build the package
python -m build

# Check that the version appears correctly in the wheel filename
# Should produce: dist/gza_agent-0.1.0-py3-none-any.whl
ls -l dist/
```

## How It Works

- **Tagged commits**: A commit tagged with `v0.1.0` builds as version `0.1.0`
- **Development versions**: Commits after the tag build as `0.1.0.dev1+gABCDEF` (with commit hash)
- **Version file**: The version will be written to `src/gza/_version.py` during build

## Usage Examples

### For stable releases:
```bash
git tag v0.2.0
git push origin v0.2.0
python -m build
# Produces: gza_agent-0.2.0-py3-none-any.whl
```

### For development snapshots:
```bash
# Just commit and build (no tag needed)
git commit -m "Add new feature"
python -m build
# Produces: gza_agent-0.1.0.dev1+g1a2b3c4-py3-none-any.whl
```

## Configuration Reference

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
