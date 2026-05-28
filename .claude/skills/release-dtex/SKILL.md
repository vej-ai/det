---
name: release-dtex
description: Cut a dtex release to PyPI and GitHub — runs the full 8-check pre-flight, bumps version + CHANGELOG, tags, publishes via Trusted Publishing, cuts the GitHub Release. Use when the user says "cut a release", "ship X.Y.Z", or any clear release-prep request.
---

# Cut a dtex release

You're cutting a release of `dtex` to PyPI + GitHub. The repo is
`vej-ai/dtex` and lives at `~/dev/simple_e/` (folder name preserved
from the simpl.E → det → detx → dtex rename history; the package
itself is `dtex`).

**Critical context:** PyPI does not allow re-publishing a deleted or
yanked version under the same number. Tag pushes are irreversible.
This skill exists because the v0.1.0 → 0.1.1 → 0.1.2 sequence each
shipped a defect the pre-flight checks would have caught locally.

## When to use this skill

Trigger phrases: "cut v0.1.4", "release X.Y.Z", "ship a new dtex
version", "prep a release". Also when the user has merged a feature
to `main` and asks "what's next" with the implication of releasing.

DO NOT use this skill for:
- Doc-only changes (those don't need a PyPI release).
- Internal refactors that don't change user-visible behavior.
- Anything not committed and pushed to `main` yet.

## The full release flow

### Phase 0 — Decide the version number

Reading the CHANGELOG and `pyproject.toml`:

- **Patch (`0.X.Y` → `0.X.Y+1`)**: pure bug fixes, no new public APIs,
  no contract changes. The 0.1.0/0.1.1/0.1.2 releases were all patches.
- **Minor (`0.X.Y` → `0.X+1.0`)**: new features, new CLI commands, new
  hooks, new baked connectors. Backward-compatible in the public API.
- **Major (`0.X.Y` → `1.0.0`)**: contract-breaking change. Don't ship a
  1.0 without explicit user direction.

Pre-1.0 latitude: dtex is in alpha; minor bumps are also a reasonable
choice for additive features. Use judgment.

### Phase 1 — Pre-flight (the 8 checks)

Run from `~/dev/simple_e`. Every check must pass.

```bash
cd ~/dev/simple_e

# Build the wheel + sdist that will go to PyPI.
rm -rf dist && .venv/bin/python -m build

# Twine's structural check — catches malformed README, bad classifier.
.venv/bin/twine check dist/*

# Install in a throwaway venv.
python3 -m venv /tmp/dtex_preflight
/tmp/dtex_preflight/bin/pip install dist/dtex-X.Y.Z-py3-none-any.whl

# --version matches the tag (drift-detection).
output=$(/tmp/dtex_preflight/bin/dtex --version)
[ "$output" = "dtex, version X.Y.Z" ] || { echo "FAIL"; exit 1; }

# Import-side __version__ matches.
/tmp/dtex_preflight/bin/python -c \
  "import dtex; assert dtex.__version__ == 'X.Y.Z'"

# README has no relative links (PyPI doesn't resolve them).
.venv/bin/python -c "
from readme_renderer.markdown import render
import re
html = render(open('README.md').read())
assert html is not None
hrefs = re.findall(r'href=\"([^\"]+)\"', html)
rel = [h for h in hrefs if not h.startswith('http')]
assert not rel, f'relative links: {rel}'"

# Entry-points wired (the three secret resolvers).
/tmp/dtex_preflight/bin/python -c "
from importlib.metadata import entry_points
schemes = sorted(ep.name for ep in entry_points(group='dtex.secret_resolvers'))
assert schemes == ['aws-secrets-manager', 'gcp-secret-manager', 'vault']"

# Tests + lint + types green.
.venv/bin/pytest -q --tb=no
.venv/bin/ruff check .
.venv/bin/mypy dtex

rm -rf /tmp/dtex_preflight
```

**If anything fails: STOP. Fix the defect. Re-run the full pre-flight.**
Do NOT proceed to tagging with a known failure.

### Phase 2 — Bump version + update CHANGELOG

```bash
# Bump pyproject.toml.
sed -i '' 's/^version = "X.Y.Z-1"$/version = "X.Y.Z"/' pyproject.toml
```

For CHANGELOG.md: use `Edit` with `old_string`/`new_string` to:

1. Promote `## [Unreleased]` content to `## [X.Y.Z] — YYYY-MM-DD` (today's
   date from the prompt environment).
2. Leave an empty `## [Unreleased]` heading above it for next time.
3. Update the version-link footers at the bottom:
   - Add `[X.Y.Z]: https://github.com/vej-ai/dtex/releases/tag/vX.Y.Z`
   - Update `[Unreleased]: https://github.com/vej-ai/dtex/compare/vX.Y.Z...HEAD`

Then **re-run the full pre-flight from Phase 1** (the wheel was built
with the old version; rebuild against the new).

### Phase 3 — Confirm with the user before the irreversible step

Before pushing the tag, surface the readiness:

> Pre-flight passed. Ready to tag and publish v`X.Y.Z`. This is irreversible
> — PyPI will not allow re-publishing this version number. Confirm and I'll
> push.

Wait for explicit confirmation. If the user is silent or asks to
review the CHANGELOG / README diff, do that. **Never skip this gate.**

### Phase 4 — Commit, push, tag, publish

```bash
git add CHANGELOG.md pyproject.toml
git -c user.name="Albinas Plesnys" -c user.email="albus@vej.ai" \
  commit -q -m "Release X.Y.Z

<one-paragraph summary, no agent commentary>

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
git push origin main

git -c user.name="Albinas Plesnys" -c user.email="albus@vej.ai" \
  tag -a vX.Y.Z -m "Release X.Y.Z — <one-line summary>

See CHANGELOG.md [X.Y.Z]."
git push origin vX.Y.Z
```

### Phase 5 — Verify the publish workflow + PyPI

```bash
# Wait for the specific v0.X.Y publish workflow to complete.
while ! gh run list --workflow=publish.yml --repo=vej-ai/dtex \
        --limit 5 --json status,headBranch 2>/dev/null | \
        grep -q "\"headBranch\":\"vX.Y.Z\".*\"status\":\"completed\""; do
  sleep 4
done
gh run list --workflow=publish.yml --repo=vej-ai/dtex --limit 3

# Verify on PyPI via JSON API (no cache).
curl -s https://pypi.org/pypi/dtex/json | python3 -c "
import json, sys
d = json.load(sys.stdin)
print('latest:', d['info']['version'])
assert d['info']['version'] == 'X.Y.Z'"

# Fresh install verification.
python3 -m venv /tmp/dtex_postrelease
/tmp/dtex_postrelease/bin/pip install --no-cache-dir dtex 2>&1 | tail -2
/tmp/dtex_postrelease/bin/dtex --version
rm -rf /tmp/dtex_postrelease
```

### Phase 6 — Cut the GitHub Release

```bash
# Extract the [X.Y.Z] CHANGELOG section.
awk '/^## \[X.Y.Z\]/{flag=1; next} /^## \[/{flag=0} flag' CHANGELOG.md \
  > /tmp/release_notes.md

gh release create vX.Y.Z \
  --repo vej-ai/dtex \
  --title "vX.Y.Z — <one-line summary>" \
  --notes-file /tmp/release_notes.md \
  --latest

rm /tmp/release_notes.md
```

### Phase 7 — Report back

Tell the user:

- PyPI URL: <https://pypi.org/project/dtex/>
- GitHub Release URL: <https://github.com/vej-ai/dtex/releases/tag/vX.Y.Z>
- `pip install dtex` now resolves to the new version.
- Any follow-up they'd want to do (yank a prior version, etc.).

## Rollback / yank reference

If a release ships with a defect:

- **Always prefer yank over delete.** Yanking marks deprecated but keeps
  installable by pin. Deleting burns the version number forever.
- The user does this manually on the PyPI UI:
  <https://pypi.org/manage/project/dtex/releases/> → version → Options →
  Yank.
- Cut the fix as the next patch version. Don't try to reuse the yanked
  number.

## Key invariants (never violate)

1. **Tag pushes are irreversible.** Get explicit user confirmation in
   Phase 3 before `git push origin vX.Y.Z`.
2. **Pre-flight before every release.** No exceptions. Eight checks,
   30-second total runtime; cheaper than yanking.
3. **Version bumps go in pyproject.toml only.** `dtex/__init__.py` reads
   `__version__` from `importlib.metadata` (fixed in 0.1.2). Do not
   hardcode the version in any other file.
4. **CHANGELOG entries describe what shipped, not how we built it.** No
   stage citations, no agent commentary, no "we" voice.
5. **Co-Authored-By line on every commit** — `Co-Authored-By: Claude
   Opus 4.7 (1M context) <noreply@anthropic.com>` — for honest
   attribution.

## Pointers

- Long-form runbook (for humans):
  [`docs/_internal/release.md`](../../docs/_internal/release.md).
- Workflow files: `.github/workflows/ci.yml`, `.github/workflows/publish.yml`.
- PyPI Trusted Publisher config (one-time setup, already done):
  <https://pypi.org/manage/account/publishing/>.
- GitHub Environment `pypi` config (one-time setup, already done):
  <https://github.com/vej-ai/dtex/settings/environments>.
