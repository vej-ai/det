# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Tests for ``dtex skills install`` + the first-run hint (Commit D)."""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from dtex.cli import _skills, cli


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def _show(result):  # type: ignore[no-untyped-def]
    return f"exit={result.exit_code} output={result.output!r}"


# ==========================================================================
# bundled_skill_files — the package-data source of truth
# ==========================================================================


def test_bundled_skill_files_returns_three_markdown_files() -> None:
    """The package ships dtex-write-config, dtex-write-connector, dtex-debug."""
    files = _skills.bundled_skill_files()
    names = {name for name, _ in files}
    assert names == {
        "dtex-write-config.md",
        "dtex-write-connector.md",
        "dtex-debug.md",
    }


def test_bundled_skill_files_contents_non_empty() -> None:
    """Every bundled skill has a non-trivial body (catches accidental empty files)."""
    for name, body in _skills.bundled_skill_files():
        assert len(body) > 500, f"{name} looks empty / placeholder ({len(body)} chars)"
        assert "---" in body, f"{name} is missing frontmatter delimiters"


# ==========================================================================
# install_skills_into — pure helper, no Click
# ==========================================================================


def test_install_writes_every_bundled_skill(tmp_path: Path) -> None:
    """install_skills_into copies every bundled skill into the target tree."""
    written = _skills.install_skills_into(tmp_path)
    target = tmp_path / ".claude" / "skills" / "dtex"
    assert target.is_dir()
    assert len(written) == len(_skills.bundled_skill_files())
    for path in written:
        assert path.parent == target
        assert path.read_text(encoding="utf-8")


def test_install_refuses_overwrite_without_force(tmp_path: Path) -> None:
    """A second install over the same dir raises FileExistsError by default."""
    _skills.install_skills_into(tmp_path)
    with pytest.raises(FileExistsError, match="already exists"):
        _skills.install_skills_into(tmp_path)


def test_install_force_overwrites(tmp_path: Path) -> None:
    """``force=True`` clobbers existing skill files."""
    _skills.install_skills_into(tmp_path)
    # Mutate one of the installed files so we can prove force restored it.
    target = tmp_path / ".claude" / "skills" / "dtex" / "dtex-debug.md"
    target.write_text("STALE", encoding="utf-8")
    _skills.install_skills_into(tmp_path, force=True)
    assert "STALE" not in target.read_text(encoding="utf-8")
    assert "Debugging a dtex run" in target.read_text(encoding="utf-8")


# ==========================================================================
# find_dtex_project_root — walk-up discovery
# ==========================================================================


def test_find_root_returns_project_dir(tmp_path: Path) -> None:
    """find_dtex_project_root walks up to the nearest dtex_project.yml."""
    (tmp_path / "dtex_project.yml").write_text("name: p\n")
    sub = tmp_path / "deep" / "nested" / "sub"
    sub.mkdir(parents=True)
    assert _skills.find_dtex_project_root(sub) == tmp_path


def test_find_root_returns_none_outside_project(tmp_path: Path) -> None:
    """Outside any dtex project, the helper returns None (no hint at root)."""
    assert _skills.find_dtex_project_root(tmp_path) is None


# ==========================================================================
# has_been_prompted / mark_prompted — the first-run-marker mechanics
# ==========================================================================


def test_prompted_marker_lifecycle(tmp_path: Path) -> None:
    """mark_prompted writes the marker; has_been_prompted reads it; idempotent."""
    assert _skills.has_been_prompted(tmp_path) is False
    _skills.mark_prompted(tmp_path)
    assert _skills.has_been_prompted(tmp_path) is True
    # Idempotent: re-marking is a no-op.
    _skills.mark_prompted(tmp_path)
    assert (tmp_path / ".dtex" / "skills-prompted").is_file()


def test_has_installed_skills_false_when_dir_absent(tmp_path: Path) -> None:
    assert _skills.has_installed_skills(tmp_path) is False


def test_has_installed_skills_false_when_dir_empty(tmp_path: Path) -> None:
    """An empty skills dir is treated as not-installed (recover from partial install)."""
    (tmp_path / ".claude" / "skills" / "dtex").mkdir(parents=True)
    assert _skills.has_installed_skills(tmp_path) is False


def test_has_installed_skills_true_after_install(tmp_path: Path) -> None:
    _skills.install_skills_into(tmp_path)
    assert _skills.has_installed_skills(tmp_path) is True


# ==========================================================================
# CLI surface
# ==========================================================================


def test_skills_install_cli_lands_files(runner: CliRunner, tmp_path: Path) -> None:
    """``dtex skills install <dir>`` writes every bundled skill."""
    result = runner.invoke(cli, ["skills", "install", str(tmp_path)])
    assert result.exit_code == 0, _show(result)
    target = tmp_path / ".claude" / "skills" / "dtex"
    assert target.is_dir()
    assert len(list(target.glob("*.md"))) == len(_skills.bundled_skill_files())


def test_skills_install_cli_default_directory(
    runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``dtex skills install`` with no arg targets the current directory."""
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(cli, ["skills", "install"])
    assert result.exit_code == 0, _show(result)
    assert (tmp_path / ".claude" / "skills" / "dtex").is_dir()


def test_skills_install_cli_refuses_overwrite(
    runner: CliRunner, tmp_path: Path
) -> None:
    """A second install errors cleanly without --force."""
    runner.invoke(cli, ["skills", "install", str(tmp_path)])
    result = runner.invoke(cli, ["skills", "install", str(tmp_path)])
    assert result.exit_code != 0
    assert "already exists" in result.output


def test_skills_install_cli_force_overwrites(runner: CliRunner, tmp_path: Path) -> None:
    """`--force` overwrites existing skill files."""
    runner.invoke(cli, ["skills", "install", str(tmp_path)])
    stale = tmp_path / ".claude" / "skills" / "dtex" / "dtex-debug.md"
    stale.write_text("STALE", encoding="utf-8")
    result = runner.invoke(cli, ["skills", "install", str(tmp_path), "--force"])
    assert result.exit_code == 0, _show(result)
    assert "STALE" not in stale.read_text(encoding="utf-8")


def test_skills_list_marks_installed(runner: CliRunner, tmp_path: Path) -> None:
    """`dtex skills list` shows ✓ for installed skills, blank for missing."""
    result_before = runner.invoke(
        cli, ["skills", "list", "--project-dir", str(tmp_path)]
    )
    assert result_before.exit_code == 0, _show(result_before)
    assert "[ ] dtex-debug.md" in result_before.output

    runner.invoke(cli, ["skills", "install", str(tmp_path)])
    result_after = runner.invoke(
        cli, ["skills", "list", "--project-dir", str(tmp_path)]
    )
    assert result_after.exit_code == 0, _show(result_after)
    assert "[✓] dtex-debug.md" in result_after.output


def test_skills_install_marks_project_prompted(
    runner: CliRunner, tmp_path: Path
) -> None:
    """A successful install drops the prompted marker so the hint stays quiet."""
    (tmp_path / "dtex_project.yml").write_text("name: p\n")
    result = runner.invoke(cli, ["skills", "install", str(tmp_path)])
    assert result.exit_code == 0, _show(result)
    assert _skills.has_been_prompted(tmp_path) is True
