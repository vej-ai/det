# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""`dtex skills install` — copy bundled Claude skills into a project.

The skills (``dtex/skills/*.md``) ship inside the wheel — pip puts them on
disk at ``<site-packages>/dtex/skills/`` on install. This module exposes the
``dtex skills install [DIRECTORY]`` command that copies them into
``<DIRECTORY>/.claude/skills/dtex/`` so Claude Code (or any other agent
runtime that reads ``.claude/skills/``) picks them up automatically.

pip intentionally does not run post-install scripts, so we cannot install
the skills automatically at ``pip install`` time. The closest legal
substitutes are:

  * The explicit ``dtex skills install`` command (this module).
  * The first-run hint emitted by :func:`maybe_print_first_run_hint` (see
    :mod:`dtex.cli.__init__`) — one line, once per project.

Both work off the same source-of-truth: :func:`bundled_skill_files`, which
discovers the .md files via :mod:`importlib.resources` so it works in
wheels, sdists, editable installs, and zip imports.
"""

from __future__ import annotations

from importlib import resources
from pathlib import Path

SKILLS_PACKAGE = "dtex.skills"
"""The Python package the bundled skill files live in (``dtex/skills/``)."""

DEFAULT_SKILLS_TARGET = Path(".claude") / "skills" / "dtex"
"""Where ``dtex skills install`` lands the files inside a project.

Claude Code reads ``.claude/skills/<skill-name>/SKILL.md`` per its
convention; per-tool subfolders keep dtex skills isolated from any
hand-authored ones the user already has.
"""

FIRST_RUN_MARKER = Path(".dtex") / "skills-prompted"
"""A marker file the runtime writes once it has shown the first-run hint.

Lives alongside the rest of the disposable working directory. Its presence
suppresses the hint on every subsequent ``dtex`` invocation in this
project — non-interactive, no surprises (see plan §11.3, settled
2026-06-03).
"""


def bundled_skill_files() -> list[tuple[str, str]]:
    """Return the bundled ``(filename, contents)`` pairs for every skill.

    Uses :mod:`importlib.resources` so it works regardless of how dtex
    was installed (regular wheel, sdist, editable, zipped). Reads the
    full contents into memory — the skill files are KB-sized, not MB,
    so this is fine.
    """
    skills = resources.files(SKILLS_PACKAGE)
    out: list[tuple[str, str]] = []
    for entry in sorted(skills.iterdir(), key=lambda p: p.name):
        if entry.name.endswith(".md") and not entry.name.startswith("_"):
            out.append((entry.name, entry.read_text(encoding="utf-8")))
    return out


def install_skills_into(
    directory: Path,
    *,
    force: bool = False,
) -> list[Path]:
    """Copy every bundled skill file into ``<directory>/.claude/skills/dtex/``.

    Refuses to overwrite an existing skill file unless ``force=True`` — same
    convention as :func:`dtex.cli._scaffold.scaffold_project` (no surprise
    file mutations under the user's tree). Returns the list of paths the
    install touched (created or overwritten).

    The target directory tree is created if absent.
    """
    target_dir = directory / DEFAULT_SKILLS_TARGET
    target_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    for filename, contents in bundled_skill_files():
        dest = target_dir / filename
        if dest.exists() and not force:
            raise FileExistsError(
                f"{dest} already exists; pass --force to overwrite"
            )
        dest.write_text(contents, encoding="utf-8")
        written.append(dest)
    return written


def find_dtex_project_root(start: Path) -> Path | None:
    """Walk up from ``start`` looking for the nearest ``dtex_project.yml``.

    Returns the directory containing it, or ``None`` if no project root is
    found before hitting the filesystem root. Used by the first-run hint to
    decide "is this invocation happening inside a dtex project?" — outside
    a project, the hint is silent (a global ``dtex --help`` shouldn't nag
    about skills the user has no project to install them into).
    """
    current = start.resolve()
    while True:
        if (current / "dtex_project.yml").is_file():
            return current
        if current.parent == current:
            return None
        current = current.parent


def has_installed_skills(project_root: Path) -> bool:
    """Return ``True`` iff at least one bundled skill is present in the project.

    Checks for the *target directory* with at least one ``.md`` file, not
    just the existence of an empty folder — an empty ``.claude/skills/dtex/``
    is treated as not-installed (a partial earlier install can be re-completed
    by re-running ``dtex skills install``).
    """
    target = project_root / DEFAULT_SKILLS_TARGET
    if not target.is_dir():
        return False
    return any(p.suffix == ".md" for p in target.iterdir())


def has_been_prompted(project_root: Path) -> bool:
    """Return ``True`` iff the first-run hint already fired for this project."""
    return (project_root / FIRST_RUN_MARKER).is_file()


def mark_prompted(project_root: Path) -> None:
    """Record that the first-run hint has fired — suppresses it from now on.

    Creates ``.dtex/skills-prompted`` (and the ``.dtex/`` parent if absent).
    Idempotent: re-marking a project that's already been marked is a no-op.
    The marker's contents are intentionally empty — its existence is the
    signal, not its body.
    """
    marker = project_root / FIRST_RUN_MARKER
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.touch()


def list_skills(directory: Path) -> list[Path]:
    """Return the bundled skill paths if installed into ``directory``, else ``[]``."""
    target = directory / DEFAULT_SKILLS_TARGET
    if not target.is_dir():
        return []
    return sorted(p for p in target.iterdir() if p.suffix == ".md")
