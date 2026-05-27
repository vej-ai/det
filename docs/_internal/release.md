# Release runbook (maintainer-only)

This is the internal checklist for cutting a `dtex` release to PyPI. It is
**not user-facing** — public release info lives in
[`CHANGELOG.md`](../../CHANGELOG.md) and the GitHub Releases page.

Releases are published by `.github/workflows/publish.yml`, which triggers
on any `vX.Y.Z*` tag push and uses **PyPI Trusted Publishing** (OIDC) — no
API token is stored in GitHub secrets.

---

## One-time setup

These steps run **once, before the very first release**. After that, every
release follows the per-release checklist below.

### 1. Register the Trusted Publisher on PyPI

1. Go to <https://pypi.org/manage/account/publishing/>.
2. Click **"Add a new pending publisher"** (it is "pending" because the
   `dtex` project does not exist on PyPI yet — it will be created by the
   first successful publish from this workflow).
3. Fill in:
   - **PyPI Project Name**: `dtex`
   - **Owner**: `vej-ai`
   - **Repository name**: `dtex`
   - **Workflow name**: `publish.yml`
   - **Environment name**: `pypi`

   The environment name must exactly match the `environment:` field in
   `.github/workflows/publish.yml`. Mismatch → PyPI rejects the OIDC token.
4. Save.

### 2. Create the GitHub Environment

1. In the GitHub repo: **Settings → Environments → New environment**.
2. Name it `pypi` (same string as above).
3. *Optional but recommended for v1.0:* add a **manual-approval gate** —
   "Required reviewers" = your GitHub user. The publish job will then wait
   for you to click "Approve" before uploading. For v0.1 we leave this off
   so the tag push fully drives the release; consider enabling for v1.0.
4. **No secrets to add.** Trusted Publishing handles auth entirely via
   OIDC — there is no `PYPI_API_TOKEN` to store anywhere.

---

## Per-release checklist

For every release after the one-time setup:

1. **Update `CHANGELOG.md`.** Move items from the `[Unreleased]` section
   into a new `[0.X.Y] — YYYY-MM-DD` heading. Leave an empty
   `[Unreleased]` placeholder on top.
2. **Bump the version in `pyproject.toml`** to `version = "0.X.Y"`.
3. **Commit:**
   ```sh
   git commit -m "Release 0.X.Y"
   ```
4. **Tag:**
   ```sh
   git tag -a v0.X.Y -m "Release 0.X.Y"
   ```
5. **Push commit and tag:**
   ```sh
   git push origin main
   git push origin v0.X.Y
   ```
6. The tag push fires `publish.yml`. It builds the sdist + wheel with
   hatchling, runs `twine check`, and uploads to PyPI via OIDC. Wait
   ~2-3 minutes; verify the new version at
   <https://pypi.org/project/dtex/>.
7. **Cut the GitHub Release.** On GitHub → **Releases → Draft a new
   release** → select the `v0.X.Y` tag → paste the matching CHANGELOG
   section as the release notes → **Publish release**.

---

## Rollback (when a release is broken)

PyPI does **not allow re-uploading a deleted version**. If a release ships
broken:

1. Fix the bug on `main`.
2. Cut `v0.X.(Y+1)` following the normal per-release checklist.
3. On PyPI, navigate to the broken version's page → **Manage → Yank
   release**. Add a short reason ("Broken: …; use 0.X.(Y+1) or later").
   Yanking hides the version from `pip install dtex` resolutions for new
   installs but leaves it pinnable for anyone who already depends on it
   explicitly — the right semantics for "this exists but do not use it".
4. Leave the original CHANGELOG entry in place. Add a short
   "**Yanked**: …" note under it explaining what broke and which version
   replaces it.

---

## What the publish workflow does, in one paragraph

`publish.yml` runs on `runs-on: ubuntu-latest`. It checks out the tagged
commit, sets up Python 3.13, installs `build` and `twine`, runs
`python -m build` (hatchling produces `dist/*.whl` + `dist/*.tar.gz`),
runs `twine check dist/*` to catch malformed metadata, then hands `dist/`
to `pypa/gh-action-pypi-publish@release/v1`. The publish action consumes
the OIDC token granted by `permissions: id-token: write` and exchanges it
with PyPI for an ephemeral upload credential — no long-lived secret ever
exists. The `environment: pypi` line binds the run to the GitHub
Environment, which is what PyPI's Trusted Publisher config matches on.
