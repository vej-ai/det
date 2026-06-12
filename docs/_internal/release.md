# Release runbook (maintainer-only)

This is the internal checklist for cutting a `dtex` release to PyPI and
GitHub. It is **not user-facing** — public release info lives in
[`CHANGELOG.md`](../../CHANGELOG.md) and the GitHub Releases page.

## The deployment model in one paragraph

Every push to `main` triggers `.github/workflows/ci.yml` — a pytest matrix
on Python 3.11/3.12/3.13 plus ruff + mypy. Every tag matching
`v[0-9]+.[0-9]+.[0-9]+*` triggers `.github/workflows/publish.yml` — builds
the wheel + sdist with hatchling, runs `twine check`, then uploads to PyPI
using OIDC-based [Trusted Publishing](https://docs.pypi.org/trusted-publishers/).
No PyPI API token lives in GitHub Secrets; the trust relationship is
configured once on the PyPI side.

## One-time setup (done once, ever)

### 1. PyPI Trusted Publisher

1. Go to <https://pypi.org/manage/account/publishing/>.
2. Click "Add a new pending publisher" (or, after the first release, edit
   the existing publisher).
3. Fill in:
   - **PyPI Project Name:** `dtex`
   - **Owner:** `vej-ai`
   - **Repository name:** `dtex`
   - **Workflow name:** `publish.yml`
   - **Environment name:** `pypi`
4. Save.

### 2. GitHub Environment

1. Go to <https://github.com/vej-ai/dtex/settings/environments>.
2. Click "New environment" → name it `pypi` → Save.
3. No secrets needed (Trusted Publishing handles auth via OIDC).
4. Optional: add yourself as a "Required reviewer" for the environment.
   Recommended for `v1.0.0` and later; optional for `0.x` releases.

After steps 1 + 2 you never touch them again.

## Per-release flow

### Phase 1 — Local pre-flight (the discipline)

Before pushing the tag — which is **irreversible**, because PyPI does not
allow re-uploading the same version number — every one of these checks
must pass. The 0.1.0 → 0.1.1 → 0.1.2 release sequence taught us this the
hard way: each was a real defect that the relevant pre-flight would have
caught locally.

Run these as one block (~60 seconds; the throwaway venv is the slow step):

```bash
cd ~/dev/simple_e

# Abort on the FIRST failing check. Without this, a mid-block failure
# (e.g. ruff) is swallowed and the block still exits 0 via the final
# cleanup — exactly how 0.2.0–0.2.4 shipped with a red lint job.
set -euo pipefail

# 1. Build the wheel + sdist that will actually go to PyPI.
rm -rf dist && .venv/bin/python -m build

# 2. Twine's structural check (catches malformed README, missing classifier,
#    unparseable metadata). Both wheel and sdist must PASSED.
.venv/bin/twine check dist/*

# 3. Install the wheel in a fresh venv — exactly what a user would do.
python3 -m venv /tmp/dtex_preflight
/tmp/dtex_preflight/bin/pip install dist/dtex-X.Y.Z-py3-none-any.whl

# 4. CLI --version reports the EXACT version we're about to publish.
#    Drift between pyproject.toml and dtex/__init__.py was the 0.1.0/0.1.1
#    bug. The version constant is now read from importlib.metadata, so
#    drift is impossible — but verify anyway.
[ "$(/tmp/dtex_preflight/bin/dtex --version)" = "dtex, version X.Y.Z" ] \
  || { echo "FAIL: --version mismatch"; exit 1; }

# 5. Import-side __version__ matches.
/tmp/dtex_preflight/bin/python -c \
  "import dtex; assert dtex.__version__ == 'X.Y.Z', dtex.__version__"

# 6. Render README as PyPI would; assert no relative links survived.
#    Relative links (./docs/..., ./LICENSE) work on GitHub but render
#    broken on PyPI. That was the 0.1.0 bug.
.venv/bin/python -c "
from readme_renderer.markdown import render
import re
html = render(open('README.md').read())
assert html is not None, 'README is not valid'
hrefs = re.findall(r'href=\"([^\"]+)\"', html)
rel = [h for h in hrefs if not h.startswith('http')]
assert not rel, f'relative links would break on PyPI: {rel}'
print(f'README ok ({len(hrefs)} hrefs)')"

# 7. Entry-points are wired (the three secret-manager resolvers).
/tmp/dtex_preflight/bin/python -c "
from importlib.metadata import entry_points
schemes = sorted(ep.name for ep in entry_points(group='dtex.secret_resolvers'))
assert schemes == ['aws-secrets-manager', 'gcp-secret-manager', 'vault'], schemes
print('entry-points ok')"

# 8. Full test suite.
.venv/bin/pytest -q --tb=no

# 9. Lint — CI's ruff + mypy job runs these exact commands; red here
#    means red CI on main.
.venv/bin/ruff check .

# 10. Types.
.venv/bin/mypy dtex

# Clean up.
rm -rf /tmp/dtex_preflight
echo "PRE-FLIGHT PASSED (all 10 checks)"
```

If anything above fails, **fix it first**. Don't continue to Phase 2.

### Phase 2 — Bump the version + update CHANGELOG

1. Edit `pyproject.toml`:
   ```toml
   version = "X.Y.Z"
   ```
2. Edit `CHANGELOG.md`:
   - Move items from `## [Unreleased]` into a new `## [X.Y.Z] — YYYY-MM-DD`
     heading right below it.
   - Add a `[X.Y.Z]: https://github.com/vej-ai/dtex/releases/tag/vX.Y.Z`
     link reference near the bottom.
   - Update the `[Unreleased]` link reference to point at `vX.Y.Z...HEAD`.
3. **Skills maintenance contract (streams-redesign-plan §11.5):** if
   this release changes the config schema, the connector contract, or
   the destination contract, the matching bundled skill file
   (`dtex/skills/dtex-write-config.md`,
   `dtex/skills/dtex-write-connector.md`,
   or `dtex/skills/dtex-debug.md`) **must be updated in the same
   release**. The skills ship inside the wheel and teach the schema —
   a release that changes the schema without updating the skills will
   teach Claude the old shape until the next release. Verify by
   diffing `dtex/types.py`, `dtex/registry.py`, and
   `dtex/destinations/*/destination.py` against the prior tag; any
   schema-relevant diff implies a skill-file diff.
4. Re-run **Phase 1** (the pre-flight) — the wheel was built with the old
   version; rebuild and re-check.

### Phase 3 — Commit + push the release prep

```bash
git add CHANGELOG.md pyproject.toml
git commit -m "Release X.Y.Z"
git push origin main
```

This commit by itself does NOT publish. It just triggers CI on `main`.
Wait for CI to go green (~3-4 min) — this gate is **mandatory**:

```bash
sleep 15  # let the push-triggered run register
gh run watch --repo vej-ai/dtex --exit-status \
  "$(gh run list --repo vej-ai/dtex --workflow ci.yml --branch main \
       --limit 1 --json databaseId --jq '.[0].databaseId')"
```

If it exits non-zero, CI is red: fix on `main` and re-run the Phase 1
pre-flight before tagging — tagging a red commit publishes a broken
release.

### Phase 4 — Tag + push (the irreversible step)

```bash
git tag -a vX.Y.Z -m "Release X.Y.Z — <one-line summary>"
git push origin vX.Y.Z
```

The tag push triggers `publish.yml`. Watch the workflow run:

```bash
gh run watch --repo vej-ai/dtex
```

Or check the Actions tab on GitHub. Typical timing: ~30-90s.

When it finishes successfully, verify:

```bash
# PyPI's JSON API (no cache).
curl -s https://pypi.org/pypi/dtex/json | python3 -c "
import json, sys
d = json.load(sys.stdin)
print('latest:', d['info']['version'])"

# Fresh install from PyPI.
python3 -m venv /tmp/dtex_postrelease
/tmp/dtex_postrelease/bin/pip install --no-cache-dir dtex
/tmp/dtex_postrelease/bin/dtex --version
rm -rf /tmp/dtex_postrelease
```

### Phase 5 — Cut the GitHub Release

The PyPI release is live; now mirror it on GitHub. From the CLI:

```bash
# Extract the [X.Y.Z] section body for the release notes.
awk '/^## \[X.Y.Z\]/{flag=1; next} /^## \[/{flag=0} flag' CHANGELOG.md \
  > /tmp/release_notes.md

gh release create vX.Y.Z \
  --repo vej-ai/dtex \
  --title "vX.Y.Z — <one-line summary>" \
  --notes-file /tmp/release_notes.md \
  --latest

rm /tmp/release_notes.md
```

Or, equivalently, on the web: <https://github.com/vej-ai/dtex/releases> →
"Draft a new release" → select the `vX.Y.Z` tag → paste the matching
CHANGELOG section → publish.

The GitHub Release powers:

- The repo's "Releases" tab and the sidebar widget.
- The `--latest` badge in the README (which already points at
  `actions/workflows/ci.yml/badge.svg`, but the release shows up in the
  repo header).
- The RSS / Atom feed for watchers.

## Rollback / yank

PyPI has two rollback paths. Pick by impact:

- **Yank a version** (reversible, the right move 99% of the time): mark a
  release as deprecated. `pip install dtex` skips it for "latest"
  resolution; existing pins still work and produce a yellow banner.
  - Go to <https://pypi.org/manage/project/dtex/releases/>.
  - Click the version → "Options" → "Yank".
  - The version number stays claimed — you can never re-publish that
    exact number.

- **Delete a version** (irreversible, almost never): removes the version
  from PyPI's index entirely. `pip install dtex==<version>` then fails.
  - Same UI, "Delete this release".
  - The version number is still burned forever (PyPI prevents re-upload
    even after deletion).
  - The only reason to delete instead of yank is if the wheel contains
    credentials, secrets, or data that must not be retrievable. Otherwise
    yank.

After yanking, cut the fix as the next patch version (`X.Y.Z+1`) and run
the full per-release flow.

## Lessons from the 0.1.0 → 0.1.1 → 0.1.2 → 0.1.3 sequence

The first three releases each failed Phase 1 in a way that would have
been caught locally:

- **0.1.0** shipped a README with relative links (`./docs/...`,
  `./LICENSE`). They rendered as broken on PyPI's project page. Pre-flight
  check 6 catches this.
- **0.1.1** shipped a `dtex --version` that lied (reported `0.1.0`)
  because `dtex/__init__.py` hardcoded the version and drifted from
  `pyproject.toml`. The structural fix in 0.1.2 was to read from
  `importlib.metadata`. Pre-flight check 4 catches the drift case.
- **0.1.2** was clean — verified by running all pre-flight checks
  before tagging.
- **0.1.3** added multi-file project-local connectors; the pre-flight ran
  cleanly first try.
- **0.2.0 through 0.2.4** all shipped while the `ruff + mypy` CI job was
  red. The lint/type checks were *in* the pre-flight, but the block ran
  without `set -e`, so their failures were swallowed and the block
  exited 0. Two fixes: the pre-flight block now starts with
  `set -euo pipefail`, and tagging now requires a green CI run on
  `main` first (the Phase 3 wait is mandatory, not advisory).

The pre-flight battery in Phase 1 is the single most important part of
this runbook. Do not skip it.

## Quick reference

| Step | Command | Reversible? |
|---|---|---|
| Pre-flight | the 10 checks above | yes |
| Commit release prep | `git commit && git push origin main` | yes (rebase + force-push, ugly but works) |
| Tag + publish | `git tag vX.Y.Z && git push origin vX.Y.Z` | **NO — burns the version number on PyPI** |
| GitHub Release | `gh release create vX.Y.Z ...` | yes (`gh release delete vX.Y.Z`) |
| Yank | PyPI UI → Releases → version → Yank | yes (un-yank in same UI) |
| Delete | PyPI UI → Releases → version → Delete | **NO — burns the version forever** |
