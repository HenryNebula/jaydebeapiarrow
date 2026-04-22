# DevOps Guide: Release and Pre-Release Workflow

This document outlines the workflow for creating pre-releases (Release Candidates) for User Acceptance Testing (UAT) and final releases using `bump-my-version`.

## Prerequisites

`bump-my-version` is included as a project dependency. Use `uv` to run it:

```bash
uv sync
```

## `[tool.bumpversion]` Configuration

The versioning configuration lives in `pyproject.toml` under `[tool.bumpversion]`. It handles release phases (`dev`, `rc`, `final`).

```toml
[tool.bumpversion]
current_version = "2.1.1"
commit = true
tag = true
parse = "(?P<major>\\d+)\\.(?P<minor>\\d+)\\.(?P<patch>\\d+)(?:(?P<release>[a-zA-Z]+)(?P<build>\\d+))?"
serialize = [
    "{major}.{minor}.{patch}{release}{build}",
    "{major}.{minor}.{patch}",
]

[[tool.bumpversion.files]]
filename = "pyproject.toml"

[tool.bumpversion.parts.release]
optional_value = "final"
values = ["dev", "rc", "final"]
```

## Workflow: From Dev to UAT to Production

This workflow allows you to seamlessly transition from development builds to Release Candidates (RCs) for testing, and finally to production releases, without manually editing version strings.

### 1. Creating a Release Candidate (RC) for UAT

When the `dev` branch is ready for User Acceptance Testing (UAT), cut a Release Candidate.

```bash
# This will update the version (e.g., from 2.1.1 to 2.1.2rc1), commit, and tag.
uv run bump-my-version bump patch --new-version 2.1.2rc1
```

Push the commit and the new tag to GitHub:

```bash
git push origin dev --tags
```

**What happens next?**
If your GitHub Actions are configured to trigger on release tags, this will publish the `2.1.2rc1` version to PyPI as a pre-release.

**How UAT testers install it:**
Testers must explicitly request pre-releases using pip's `--pre` flag:

```bash
pip install --pre JayDeBeApiArrow
```

### 2. Incrementing the RC (Iterating during UAT)

If UAT uncovers bugs, fix them on the `dev` branch, commit the fixes, and cut a new RC.

```bash
# This automatically bumps the build number (e.g., 2.1.2rc1 -> 2.1.2rc2), commits, and tags.
uv run bump-my-version bump build
```

Push the changes and the new tag:

```bash
git push origin dev --tags
```

### 3. Making the Final Release

Once UAT is approved and the RC is confirmed stable:

1. Merge the `dev` branch into `main`.
2. Checkout the `main` branch.
3. Run the bump command to drop the `rc` suffix.

```bash
# This transitions the version from 'rc' to 'final' (which is the optional_value and omitted).
# Example: 2.1.2rc2 -> 2.1.2. It will commit and tag automatically.
uv run bump-my-version bump release
```

Push the final release to GitHub:

```bash
git push origin main --tags
```

This will trigger the final production release to PyPI via GitHub Actions.
