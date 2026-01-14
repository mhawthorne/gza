# Tag the release and push it to the remote repository:
```bash
git tag v0.1.0
git push origin v0.1.0
```

# Create a GitHub release (this triggers the automated publish):
```bash
gh release create v0.1.0 --title "v0.1.0" --notes "Release notes here"
```

# show all commits between 2 tags:
```bash
git log v0.1.0..v0.2.0 --oneline
```

# For a nice format suitable for release notes:
```bash
git log v0.1.0..v0.2.0 --pretty=format:"- %s" --no-merges
```