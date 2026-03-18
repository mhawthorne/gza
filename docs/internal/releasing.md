# Releasing

## Generate release notes

### Option A: Generate against HEAD (preferred)

This avoids the chicken-and-egg problem of needing a tag before you can generate notes.

```bash
./bin/generate-release-notes.sh v0.3.0 HEAD
# Review and edit docs/release-notes/HEAD.md
mv docs/release-notes/HEAD.md docs/release-notes/v0.4.0.md
```

### Option B: Generate between two existing tags

If both tags already exist:

```bash
./bin/generate-release-notes.sh v0.3.0 v0.4.0
```

This uses Claude to analyze commits between the two tags and writes categorized markdown to `docs/release-notes/<tag>.md`.

## Tag and publish

```bash
# Commit the release notes
git add docs/release-notes/v0.4.0.md
git commit -m "add v0.4.0 release notes"

# Tag the release (use -f if the tag already exists)
git tag v0.4.0
git push origin v0.4.0

# Create a GitHub release (triggers automated publish)
gh release create v0.4.0 --title "v0.4.0" --notes-file docs/release-notes/v0.4.0.md
```

## Useful commands

```bash
# Show all commits between 2 tags:
git log v0.3.0..v0.4.0 --oneline

# For a nice format suitable for release notes:
git log v0.3.0..v0.4.0 --pretty=format:"- %s" --no-merges

# Move an existing tag to current commit:
git tag -f v0.4.0
```

## Packaging versioning note

`pyproject.toml` uses Hatch's VCS version source (`[tool.hatch.version] source = "vcs"`) for
distribution metadata. We intentionally do not configure a Hatch VCS build hook that writes
`src/gza/_version.py`, so editable installs work when the gza source tree is mounted read-only.
