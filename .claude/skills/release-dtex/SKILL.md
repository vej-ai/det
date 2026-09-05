---
name: release-dtex
description: Cut a dtex release to PyPI and GitHub — runs the full 10-check pre-flight, bumps version + CHANGELOG, tags, publishes via Trusted Publishing, cuts the GitHub Release. Use when the user says "cut a release", "ship X.Y.Z", or any clear release-prep request.
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

### Phase 1 — Pre-flight (the 10 checks)

Run from `~/dev/simple_e`. Every check must pass.

```bash
cd ~/dev/simple_e

# Every check is guarded EXPLICITLY with `|| fail <name>`. Do NOT rely on
# `set -e`: in the Claude Code shell a failing step inside a
# `( set -euo pipefail; ... )` block did NOT abort the block (0.10.1 — an
# AssertionError and a red pytest run were followed by "PRE-FLIGHT PASSED"),
# and 0.2.0–0.2.4 shipped red lint the same way. Never pipe a check into
# `tail`/`grep` without capturing its exit code first.
fail() { echo "PRE-FLIGHT FAILED at: $1"; exit 1; }
V=X.Y.Z
# Throwaway venv in the session scratchpad (never /tmp — the user asked).
S=<scratchpad directory from the system prompt>

# 1. Build the wheel + sdist that will go to PyPI.
rm -rf dist && .venv/bin/python -m build || fail build

# 2. Twine's structural check — catches malformed README, bad classifier.
.venv/bin/twine check dist/* || fail twine

# 3. Install in a throwaway venv. Check the console script exists: a pip
#    failure (e.g. "No space left on device" — 0.10.1) must not slip by.
rm -rf "$S/dtex_preflight" && python3 -m venv "$S/dtex_preflight" || fail venv
"$S/dtex_preflight/bin/pip" install "dist/dtex-$V-py3-none-any.whl" || fail install
[ -x "$S/dtex_preflight/bin/dtex" ] || fail "install (no dtex script)"

# 4. --version matches the tag (drift-detection).
output=$(cd "$S" && "$S/dtex_preflight/bin/dtex" --version)
[ "$output" = "dtex, version $V" ] || fail "version ($output)"

# 5. Import-side __version__ matches — AND the import really comes from the
#    wheel. Run from OUTSIDE the repo: `python -c` puts the cwd first on
#    sys.path, so from ~/dev/simple_e this silently imported the checkout
#    instead of the wheel (every release before 0.10.1 tested nothing here).
(cd "$S" && "$S/dtex_preflight/bin/python" -c "
import dtex
assert dtex.__version__ == '$V', dtex.__version__
assert 'dtex_preflight' in dtex.__file__, dtex.__file__
import dtex.cli") || fail import

# 6. README has no relative links (PyPI doesn't resolve them).
.venv/bin/python -c "
from readme_renderer.markdown import render
import re
html = render(open('README.md').read())
assert html is not None
hrefs = re.findall(r'href=\"([^\"]+)\"', html)
rel = [h for h in hrefs if not h.startswith('http')]
assert not rel, f'relative links: {rel}'" || fail readme

# 7. Entry-points wired (the three secret resolvers) — from outside the
#    repo for the same reason as check 5.
(cd "$S" && "$S/dtex_preflight/bin/python" -c "
from importlib.metadata import entry_points
schemes = sorted(ep.name for ep in entry_points(group='dtex.secret_resolvers'))
assert schemes == ['aws-secrets-manager', 'gcp-secret-manager', 'vault'], schemes") || fail entrypoints

# 8. Full test suite. Capture the exit code BEFORE tailing the output.
.venv/bin/pytest -q --tb=short -p no:cacheprovider > "$S/pytest.log" 2>&1; rc=$?
tail -1 "$S/pytest.log"; [ "$rc" -eq 0 ] || fail pytest

# 9. Lint — CI's ruff + mypy job runs these EXACT commands; red here
#    means red CI on main. NOTE: CI installs deps unpinned, so a new
#    click/mypy release can turn CI red on untouched code (click 8.5.0 did,
#    0.10.0); reproduce by upgrading the dep in .venv before blaming the diff.
.venv/bin/ruff check . || fail ruff

# 10. Types.
.venv/bin/mypy dtex || fail mypy

rm -rf "$S/dtex_preflight"
echo "PRE-FLIGHT PASSED (all 10 checks)"
```

Checks 8–10 are individually load-bearing: 0.2.0 through 0.2.4 each
shipped with a red `ruff + mypy` CI job because their failures were
silently swallowed — hence the explicit `|| fail` on every line.

Two more things the pre-flight does NOT check, learned the hard way:

- **Disk space.** 0.10.1's first pre-flight died with "No space left on
  device" inside the throwaway venv (the machine was at 100 %). Check
  `df -h /` first if anything in step 3 or 8 fails oddly.
- **Author metadata is public.** A contributor credited via
  `git commit --author` ships their email to GitHub/PyPI forever (tags are
  not rewritten). Use the contributor's GitHub noreply address
  (`<id>+<login>@users.noreply.github.com`) unless they want their real
  email public.

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

<the Co-Authored-By / Claude-Session attribution lines the session prompt specifies>"
git push origin main

# Wait for CI on main to go GREEN before tagging — tagging a red
# commit publishes a broken release. (0.2.0–0.2.4 all tagged on red
# CI because this gate was missing.) ~3-4 min.
sleep 15  # let the push-triggered run register
gh run watch --repo vej-ai/dtex --exit-status \
  "$(gh run list --repo vej-ai/dtex --workflow ci.yml --branch main \
       --limit 1 --json databaseId --jq '.[0].databaseId')"

git -c user.name="Albinas Plesnys" -c user.email="albus@vej.ai" \
  tag -a vX.Y.Z -m "Release X.Y.Z — <one-line summary>

See CHANGELOG.md [X.Y.Z]."
git push origin vX.Y.Z
```

If `gh run watch` exits non-zero, CI is red: STOP, fix on `main`,
and restart from the Phase 1 pre-flight. Do NOT push the tag.

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
2. **Pre-flight before every release.** No exceptions. Ten checks,
   ~90-second total runtime; cheaper than yanking. Every check carries an
   explicit `|| fail` — `set -e` alone did not abort a failing step in
   the Claude Code shell (0.10.1), and a swallowed mid-block failure is
   how 0.2.0–0.2.4 shipped with red lint. Import checks run from OUTSIDE
   the repo so they exercise the wheel, not the checkout.
3. **Never tag while CI on `main` is red.** The release-prep push must
   produce a green CI run before `git push origin vX.Y.Z`.
4. **Version bumps go in pyproject.toml only.** `dtex/__init__.py` reads
   `__version__` from `importlib.metadata` (fixed in 0.1.2). Do not
   hardcode the version in any other file.
5. **CHANGELOG entries describe what shipped, not how we built it.** No
   stage citations, no agent commentary, no "we" voice.
6. **Attribution trailer on every commit** — the `Co-Authored-By` (and
   `Claude-Session`) lines the current session prompt specifies, for
   honest attribution. Do not hardcode a model name here; it changes.

## Pointers

- Long-form runbook (for humans):
  [`docs/_internal/release.md`](../../docs/_internal/release.md).
- Workflow files: `.github/workflows/ci.yml`, `.github/workflows/publish.yml`.
- PyPI Trusted Publisher config (one-time setup, already done):
  <https://pypi.org/manage/account/publishing/>.
- GitHub Environment `pypi` config (one-time setup, already done):
  <https://github.com/vej-ai/dtex/settings/environments>.
