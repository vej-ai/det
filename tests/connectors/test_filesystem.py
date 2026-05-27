"""Tests for the baked ``filesystem`` source connector.

All real, no external services. Everything lives under ``tmp_path``:

* CSV / JSONL / Parquet round-trips (Parquet built inline with pyarrow, the
  test skips when pyarrow is absent).
* Multi-file glob with deterministic sort by cursor key.
* Incremental: a second run past the first file's cursor key skips it.
* A malformed CSV raises a clear error naming the file.
* End-to-end run via :func:`dtex.run` lands rows in a tmp DuckDB and
  exercises schema inference (no schema declared on the example stream).
* Backend dispatch: GCS and S3 backends are unit-tested by monkeypatching
  the lazy SDK import — no live cloud calls.
"""

from __future__ import annotations

import csv
import json
import os
import sys
import textwrap
import time
from pathlib import Path
from typing import Any

import pytest
import yaml

from dtex import Config, Cursor
from tests.conftest import load_connector

# Connector folder under the installed package — the engine resolves it
# the same way; the test imports it directly via the conftest harness.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
FILESYSTEM_CONNECTOR_DIR = (
    _REPO_ROOT / "dtex" / "sources" / "filesystem"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_csv(path: Path, rows: list[dict[str, str]], delimiter: str = ",") -> None:
    """Write a CSV with a header row at ``path`` from a list of dicts."""
    fieldnames = list(rows[0])
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=delimiter)
        writer.writeheader()
        writer.writerows(rows)


def _make_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    """Write JSONL — one ``json.dumps(row)`` per line."""
    with path.open("w") as f:
        for row in rows:
            f.write(json.dumps(row))
            f.write("\n")


def _run_stream(path: Path, **params: Any) -> list[list[dict[str, Any]]]:
    """Load the connector and invoke its ``files`` stream — return all batches.

    Mirrors what the engine does internally: build a :class:`Config` from
    the run params, build a :class:`Cursor` for the stream's declared
    incremental block, call the registered ``files`` function.
    """
    loaded = load_connector(FILESYSTEM_CONNECTOR_DIR)
    reg = loaded.registry.stream("files")
    assert reg is not None, "filesystem must register a `files` stream"

    stream_def = loaded.manifest.stream("files")
    assert stream_def is not None and stream_def.incremental is not None

    full_params: dict[str, Any] = {"path": str(path)}
    full_params.update(params)
    config = Config(params=full_params)
    cursor = Cursor(
        cursor_field=stream_def.incremental.cursor_field,
        cursor_type=stream_def.incremental.cursor_type,
        start_value=params.pop("_start_value", None),
    )
    return list(reg.func(config=config, cursor=cursor))


# ---------------------------------------------------------------------------
# Manifest sanity — register.yaml is discoverable + well-formed
# ---------------------------------------------------------------------------


def test_register_yaml_parses_and_declares_files_stream() -> None:
    """The manifest parses cleanly and the example ``files`` stream is incremental."""
    loaded = load_connector(FILESYSTEM_CONNECTOR_DIR)
    manifest = loaded.manifest

    assert manifest.name == "filesystem"
    assert manifest.kind.value == "source"
    assert manifest.version == "1.0.0"
    # No secrets in v1 — read the top-of-file NOTE for why.
    assert manifest.secrets == ()
    # Declared params we depend on at runtime.
    for name in (
        "path",
        "glob",
        "format",
        "batch_size",
        "cursor_strategy",
        "csv_delimiter",
        "csv_has_header",
    ):
        assert name in manifest.params, f"register.yaml is missing param {name!r}"

    files = manifest.stream("files")
    assert files is not None
    assert files.is_incremental
    assert files.incremental is not None
    assert files.incremental.cursor_field == "_dtex_file_cursor"
    # cursor_type is `timestamp`, not the natural `string` — see register.yaml
    # NOTE for the DuckDB JSON-binding constraint that forces this.
    assert files.incremental.cursor_type.value == "timestamp"
    # `@stream(name="files")` must be registered by source.py.
    assert "files" in loaded.registry.stream_names


# ---------------------------------------------------------------------------
# CSV — the local-only baseline path
# ---------------------------------------------------------------------------


def test_csv_local_file_yields_50_rows_in_correct_batches(tmp_path: Path) -> None:
    """A 50-row CSV reads as 50 records, batched per ``batch_size``."""
    rows = [
        {"id": str(i), "name": f"row-{i}", "amount": str(i * 1.5)}
        for i in range(1, 51)
    ]
    _make_csv(tmp_path / "data.csv", rows)

    batches = _run_stream(tmp_path, glob="**/*.csv", batch_size=20)

    flat = [r for batch in batches for r in batch]
    assert len(flat) == 50
    # 50 rows in batches of 20 → 20 + 20 + 10.
    assert [len(b) for b in batches] == [20, 20, 10]
    # CSV cells are strings — DictReader preserves text.
    assert flat[0]["id"] == "1"
    assert flat[0]["name"] == "row-1"
    # Synthetic cursor field is attached to every record.
    assert all("_dtex_file_cursor" in r for r in flat)


def test_csv_without_header_uses_col_n_keys(tmp_path: Path) -> None:
    """A header-less CSV gets column names ``col_0``, ``col_1``, ..."""
    (tmp_path / "nohdr.csv").write_text("a,1\nb,2\nc,3\n")
    batches = _run_stream(
        tmp_path, glob="**/*.csv", csv_has_header=False, batch_size=10
    )
    flat = [r for batch in batches for r in batch]
    assert len(flat) == 3
    assert flat[0] == {
        "col_0": "a",
        "col_1": "1",
        "_dtex_file_cursor": flat[0]["_dtex_file_cursor"],
    }


def test_csv_with_custom_delimiter_reads_tsv(tmp_path: Path) -> None:
    """A TSV-style file reads when ``csv_delimiter`` is set to a tab."""
    (tmp_path / "data.csv").write_text("id\tname\n1\talpha\n2\tbeta\n")
    batches = _run_stream(
        tmp_path, glob="**/*.csv", csv_delimiter="\t", batch_size=10
    )
    flat = [r for batch in batches for r in batch]
    assert [r["id"] for r in flat] == ["1", "2"]
    assert [r["name"] for r in flat] == ["alpha", "beta"]


# ---------------------------------------------------------------------------
# JSONL
# ---------------------------------------------------------------------------


def test_jsonl_local_file_streams_objects(tmp_path: Path) -> None:
    """A JSONL file with 5 objects yields 5 dict records, structure preserved."""
    _make_jsonl(
        tmp_path / "events.jsonl",
        [
            {"id": 1, "tags": ["a", "b"], "score": 10},
            {"id": 2, "tags": [], "score": 0},
            {"id": 3, "nested": {"deep": [1, 2, 3]}},
            {"id": 4, "value": None},
            {"id": 5},
        ],
    )
    batches = _run_stream(tmp_path, glob="**/*.jsonl", batch_size=100)
    flat = [r for batch in batches for r in batch]
    assert len(flat) == 5
    # Native JSON types pass through (unlike CSV which is always text).
    assert flat[0]["id"] == 1
    assert flat[0]["tags"] == ["a", "b"]
    assert flat[2]["nested"] == {"deep": [1, 2, 3]}
    assert flat[3]["value"] is None
    # Blank lines should not corrupt the stream.
    (tmp_path / "events.jsonl").open("a").write("\n   \n")
    batches2 = _run_stream(tmp_path, glob="**/*.jsonl", batch_size=100)
    assert len([r for batch in batches2 for r in batch]) == 5


def test_jsonl_non_object_line_raises_with_file_path(tmp_path: Path) -> None:
    """A bare-array / scalar line raises ValueError naming the file."""
    p = tmp_path / "bad.jsonl"
    p.write_text('{"id": 1}\n[1, 2, 3]\n')
    with pytest.raises(ValueError, match=r"bad\.jsonl"):
        list(_run_stream(tmp_path, glob="**/*.jsonl"))


# ---------------------------------------------------------------------------
# Parquet (optional dep)
# ---------------------------------------------------------------------------


def test_parquet_local_file_streams_rows(tmp_path: Path) -> None:
    """Parquet rows read end to end via pyarrow."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    table = pa.table(
        {
            "id": [1, 2, 3, 4],
            "name": ["alpha", "beta", "gamma", "delta"],
            "amount": [1.5, 2.0, 3.25, 4.0],
        }
    )
    pq.write_table(table, tmp_path / "data.parquet")

    batches = _run_stream(tmp_path, glob="**/*.parquet", batch_size=10)
    flat = [r for batch in batches for r in batch]
    assert len(flat) == 4
    assert [r["id"] for r in flat] == [1, 2, 3, 4]
    assert flat[0]["name"] == "alpha"
    assert flat[0]["amount"] == 1.5


def test_parquet_missing_pyarrow_raises_with_install_hint(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """If pyarrow is absent the ImportError points the user at re-installing dtex.

    pyarrow ships in dtex's base install (the BigQuery destination also needs
    it). The error path exists for environments where the package was
    explicitly removed; the message tells the user to reinstall, not to add
    an extra (there is no `[parquet]` extra anymore).

    Simulates the missing dep by injecting ``None`` into ``sys.modules`` —
    works regardless of whether the test env has pyarrow installed.
    """
    # Create a real .parquet file so we get *past* the file enumeration and
    # hit the lazy import in the reader. If pyarrow is missing for real we
    # cannot create one; in that case the simulation is unnecessary and the
    # native ImportError carries the expected message.
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    pq.write_table(pa.table({"id": [1]}), tmp_path / "x.parquet")

    # Force the lazy import to fail.
    monkeypatch.setitem(sys.modules, "pyarrow.parquet", None)
    with pytest.raises(ImportError, match=r"pip install dtex"):
        list(_run_stream(tmp_path, glob="**/*.parquet"))


# ---------------------------------------------------------------------------
# Multiple files + glob + sort by cursor key
# ---------------------------------------------------------------------------


def test_multiple_files_sorted_by_mtime_cursor_key(tmp_path: Path) -> None:
    """Multiple CSV files are read in cursor-key (mtime) order."""
    _make_csv(tmp_path / "b.csv", [{"id": "1"}])
    # Force the second file to have a *later* mtime than the first so the
    # sort order is unambiguous regardless of filesystem mtime resolution.
    older = time.time() - 60
    os.utime(tmp_path / "b.csv", (older, older))
    _make_csv(tmp_path / "a.csv", [{"id": "2"}])

    batches = _run_stream(tmp_path, glob="**/*.csv", batch_size=10)
    flat = [r for batch in batches for r in batch]
    # `b.csv` (older mtime) sorts first; `a.csv` (newer) sorts second.
    assert [r["id"] for r in flat] == ["1", "2"]


def test_multiple_files_sorted_by_name_strategy(tmp_path: Path) -> None:
    """With ``cursor_strategy: name`` files sort by basename (lex)."""
    _make_csv(tmp_path / "z.csv", [{"id": "1"}])
    # Even with a later mtime, lex sort puts `a.csv` first.
    older = time.time() - 60
    os.utime(tmp_path / "z.csv", (older, older))
    _make_csv(tmp_path / "a.csv", [{"id": "2"}])

    batches = _run_stream(
        tmp_path, glob="**/*.csv", cursor_strategy="name", batch_size=10
    )
    flat = [r for batch in batches for r in batch]
    assert [r["id"] for r in flat] == ["2", "1"]  # a.csv then z.csv


def test_recursive_glob_picks_up_files_in_subdirectories(tmp_path: Path) -> None:
    """A recursive glob walks subdirectories."""
    (tmp_path / "sub").mkdir()
    _make_csv(tmp_path / "top.csv", [{"id": "top"}])
    _make_csv(tmp_path / "sub" / "deep.csv", [{"id": "deep"}])

    batches = _run_stream(tmp_path, glob="**/*.csv", batch_size=10)
    flat = [r for batch in batches for r in batch]
    assert {r["id"] for r in flat} == {"top", "deep"}


# ---------------------------------------------------------------------------
# Incremental — second-run skip
# ---------------------------------------------------------------------------


def test_incremental_skips_files_with_cursor_key_at_or_below_start(tmp_path: Path) -> None:
    """A run started past the first file's cursor (mtime datetime) skips that file."""
    from datetime import UTC, datetime

    _make_csv(tmp_path / "old.csv", [{"id": "old-1"}, {"id": "old-2"}])
    older = time.time() - 120
    os.utime(tmp_path / "old.csv", (older, older))
    _make_csv(tmp_path / "new.csv", [{"id": "new-1"}])

    # Run 1: take everything, capture the cursor's observed max (a datetime).
    loaded = load_connector(FILESYSTEM_CONNECTOR_DIR)
    reg = loaded.registry.stream("files")
    stream_def = loaded.manifest.stream("files")
    assert reg is not None and stream_def is not None and stream_def.incremental is not None

    config = Config(params={"path": str(tmp_path), "glob": "**/*.csv", "batch_size": 50})
    cursor = Cursor(
        cursor_field=stream_def.incremental.cursor_field,
        cursor_type=stream_def.incremental.cursor_type,
        start_value=None,
    )
    flat = [r for batch in reg.func(config=config, cursor=cursor) for r in batch]
    assert {r["id"] for r in flat} == {"old-1", "old-2", "new-1"}
    observed_max = cursor.observed_max
    assert isinstance(observed_max, datetime)

    # Run 2: start_value = the old file's mtime datetime. The old file is
    # skipped (sort_key > start is False); only the newer file's records remain.
    old_mtime = (tmp_path / "old.csv").stat().st_mtime
    old_dt = datetime.fromtimestamp(old_mtime, tz=UTC).replace(microsecond=0)
    cursor2 = Cursor(
        cursor_field=stream_def.incremental.cursor_field,
        cursor_type=stream_def.incremental.cursor_type,
        start_value=old_dt,
    )
    flat2 = [r for batch in reg.func(config=config, cursor=cursor2) for r in batch]
    assert [r["id"] for r in flat2] == ["new-1"]

    # Run 3: start_value past every file → no records at all.
    cursor3 = Cursor(
        cursor_field=stream_def.incremental.cursor_field,
        cursor_type=stream_def.incremental.cursor_type,
        start_value=observed_max,
    )
    flat3 = [r for batch in reg.func(config=config, cursor=cursor3) for r in batch]
    assert flat3 == []


# ---------------------------------------------------------------------------
# Malformed file
# ---------------------------------------------------------------------------


def test_malformed_csv_raises_with_file_path(tmp_path: Path) -> None:
    """A malformed CSV row raises ValueError naming the file path.

    Under the reader's ``strict=True`` mode, characters after a closing quote
    inside a field raise ``csv.Error`` — exactly the kind of structural
    breakage a real malformed file presents.
    """
    p = tmp_path / "broken.csv"
    p.write_text('id,name\n1,"hello"extra\n')
    with pytest.raises(ValueError, match=r"broken\.csv"):
        list(_run_stream(tmp_path, glob="**/*.csv"))


# ---------------------------------------------------------------------------
# Format inference + explicit format
# ---------------------------------------------------------------------------


def test_format_auto_infers_from_extension(tmp_path: Path) -> None:
    """``format: auto`` (default) reads .csv as CSV and .jsonl as JSONL."""
    _make_csv(tmp_path / "a.csv", [{"k": "v"}])
    _make_jsonl(tmp_path / "b.jsonl", [{"k": "v2"}])
    # Glob both extensions in one run.
    batches = _run_stream(tmp_path, glob="**/*", batch_size=10)
    flat = [r for batch in batches for r in batch]
    # Both files contribute exactly one record.
    assert sorted(r["k"] for r in flat) == ["v", "v2"]


def test_format_auto_unknown_extension_raises(tmp_path: Path) -> None:
    """An unknown extension under ``format: auto`` raises listing valid ones."""
    (tmp_path / "x.dat").write_text("something\n")
    with pytest.raises(ValueError, match=r"cannot infer format"):
        list(_run_stream(tmp_path, glob="**/*"))


def test_explicit_format_overrides_extension(tmp_path: Path) -> None:
    """``format: jsonl`` reads a ``.txt`` file as JSONL."""
    p = tmp_path / "data.txt"
    p.write_text('{"id": 1}\n{"id": 2}\n')
    batches = _run_stream(tmp_path, glob="**/*.txt", format="jsonl", batch_size=10)
    flat = [r for batch in batches for r in batch]
    assert [r["id"] for r in flat] == [1, 2]


# ---------------------------------------------------------------------------
# Synthetic cursor field is the file's cursor key
# ---------------------------------------------------------------------------


def test_synthetic_cursor_field_matches_file_cursor_key(tmp_path: Path) -> None:
    """Every record carries ``_dtex_file_cursor`` = its file's ISO cursor key.

    Records from the same file share the same key; cursor.observe() is called
    once per file (with the typed datetime), so the observed max ends up at
    the LAST file's mtime.
    """
    from datetime import datetime

    _make_csv(tmp_path / "first.csv", [{"id": "1"}, {"id": "2"}])
    older = time.time() - 120
    os.utime(tmp_path / "first.csv", (older, older))
    _make_csv(tmp_path / "second.csv", [{"id": "3"}])

    loaded = load_connector(FILESYSTEM_CONNECTOR_DIR)
    reg = loaded.registry.stream("files")
    stream_def = loaded.manifest.stream("files")
    assert reg is not None and stream_def is not None and stream_def.incremental is not None

    config = Config(params={"path": str(tmp_path), "glob": "**/*.csv", "batch_size": 50})
    cursor = Cursor(
        cursor_field=stream_def.incremental.cursor_field,
        cursor_type=stream_def.incremental.cursor_type,
    )
    flat = [r for batch in reg.func(config=config, cursor=cursor) for r in batch]
    keys = {r["id"]: r["_dtex_file_cursor"] for r in flat}
    # Records 1 and 2 (same file) share a cursor key (ISO string).
    assert keys["1"] == keys["2"]
    # Record 3 has a different (newer) one.
    assert keys["3"] != keys["1"]
    # The observed max is a datetime (per the cursor_type: timestamp design);
    # the record field is its ISO string form.
    assert isinstance(cursor.observed_max, datetime)
    assert cursor.observed_max.isoformat() == keys["3"]


# ---------------------------------------------------------------------------
# End-to-end via dtex.run — schema inference into DuckDB
# ---------------------------------------------------------------------------


def test_end_to_end_run_lands_inferred_rows_in_duckdb(tmp_path: Path) -> None:
    """A real ``dtex.run`` infers a schema, lands rows, advances the cursor.

    No declared schema on the example stream → engine infers from the first
    batch. This is the most important test: it proves the whole pipeline
    (discovery, run, ensure_schema, write_batch, commit_state) works against
    the filesystem source.
    """
    import duckdb

    import dtex

    # Project root with this connector visible. The filesystem source is
    # baked under dtex/sources/, so a project with no local sources still
    # finds it via the baked search root.
    project_root = tmp_path / "project"
    project_root.mkdir()
    (project_root / "dtex_project.yml").write_text(
        textwrap.dedent(
            """\
            name: filesystem_e2e_test
            version: "1.0.0"
            source_paths: [sources]
            destination_paths: [destinations]
            config_paths: [configs]
            """
        )
    )
    (project_root / "profiles.yml").write_text(
        yaml.safe_dump(
            {"duckdb": {"default_target": "dev", "targets": {"dev": {}}}},
            sort_keys=False,
        )
    )
    # Bind the baked filesystem source to the baked duckdb destination via a
    # one-config-per-file under configs/.
    (project_root / "configs").mkdir()
    (project_root / "configs" / "filesystem_dev.yml").write_text(
        textwrap.dedent(
            """\
            name: filesystem_dev
            source: filesystem
            destination: duckdb
            target: dev
            """
        )
    )

    # Source data directory + a CSV with 3 rows.
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _make_csv(
        data_dir / "people.csv",
        [
            {"id": "1", "name": "alpha", "city": "amsterdam"},
            {"id": "2", "name": "beta", "city": "berlin"},
            {"id": "3", "name": "gamma", "city": "copenhagen"},
        ],
    )

    db_path = tmp_path / "warehouse.duckdb"
    result = dtex.run(
        config="filesystem_dev",
        project_dir=str(project_root),
        params_override={"path": str(data_dir), "glob": "**/*.csv", "batch_size": 100},
        destination_params_override={"path": str(db_path)},
    )
    assert result.status.value == "succeeded", (
        f"run failed: {result.error!r}"
    )
    files_result = result.stream("files")
    assert files_result is not None
    assert files_result.rows_loaded == 3

    # Rows landed in DuckDB and the synthetic cursor field made it through.
    conn = duckdb.connect(str(db_path))
    try:
        rows = conn.execute(
            "SELECT id, name, city FROM files ORDER BY id"
        ).fetchall()
        assert rows == [
            ("1", "alpha", "amsterdam"),
            ("2", "beta", "berlin"),
            ("3", "gamma", "copenhagen"),
        ]
        # The cursor column is also present (inferred as STRING / VARCHAR).
        cursor_vals = conn.execute(
            "SELECT DISTINCT _dtex_file_cursor FROM files"
        ).fetchall()
        assert len(cursor_vals) == 1, "all rows came from one file → one cursor key"
        # State row carries the cursor advance.
        state_after_run1 = conn.execute(
            "SELECT cursor_value FROM _dtex_state "
            "WHERE connector = 'filesystem' AND stream = 'files'"
        ).fetchall()
        assert len(state_after_run1) == 1
        assert state_after_run1[0][0] is not None
    finally:
        conn.close()

    # Second run: no new files → 0 rows loaded, state cursor_value must NOT
    # regress (the source re-observes the resume value so the engine writes
    # back a clean datetime, not a stale string — see source.py NOTE).
    result2 = dtex.run(
        config="filesystem_dev",
        project_dir=str(project_root),
        params_override={"path": str(data_dir), "glob": "**/*.csv", "batch_size": 100},
        destination_params_override={"path": str(db_path)},
    )
    assert result2.status.value == "succeeded"
    assert result2.stream("files").rows_loaded == 0  # type: ignore[union-attr]

    conn = duckdb.connect(str(db_path))
    try:
        state_after_run2 = conn.execute(
            "SELECT cursor_value FROM _dtex_state "
            "WHERE connector = 'filesystem' AND stream = 'files'"
        ).fetchall()
        # Cursor must be stable (no regression, no double-stamp) — the
        # serialized JSON cursor_value compares equal across runs.
        assert state_after_run2[0][0] == state_after_run1[0][0]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Backend dispatch + unit tests via monkeypatched lazy imports
# ---------------------------------------------------------------------------


def test_pick_backend_dispatches_on_scheme() -> None:
    """URI scheme → backend type. Bad scheme raises listing the valid set."""
    from dtex.sources.filesystem.backends import (
        GcsBackend,
        LocalBackend,
        S3Backend,
        pick_backend,
    )

    assert isinstance(pick_backend("/tmp/x"), LocalBackend)
    assert isinstance(pick_backend("file:///tmp/x"), LocalBackend)
    assert isinstance(pick_backend("gs://bucket/p"), GcsBackend)
    assert isinstance(pick_backend("s3://bucket/p"), S3Backend)
    with pytest.raises(ValueError, match=r"unsupported URI scheme"):
        pick_backend("ftp://host/path")


def test_gcs_backend_missing_dep_raises_with_install_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing google-cloud-storage raises ImportError naming ``dtex[gcs]``."""
    from dtex.sources.filesystem.backends import GcsBackend

    # Pretend the SDK is not importable — sentinel `None` triggers ImportError
    # in `from google.cloud import storage`.
    monkeypatch.setitem(sys.modules, "google.cloud.storage", None)
    monkeypatch.setitem(sys.modules, "google.cloud", None)
    backend = GcsBackend(bucket="b", prefix="p")
    with pytest.raises(ImportError, match=r"dtex\[gcs\]"):
        backend.list_files("gs://b/p", "**/*.csv", cursor_strategy="mtime")


def test_s3_backend_missing_dep_raises_with_install_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing boto3 raises ImportError naming ``dtex[s3]``."""
    from dtex.sources.filesystem.backends import S3Backend

    monkeypatch.setitem(sys.modules, "boto3", None)
    backend = S3Backend(bucket="b", prefix="p")
    with pytest.raises(ImportError, match=r"dtex\[s3\]"):
        backend.list_files("s3://b/p", "**/*.csv", cursor_strategy="mtime")


def test_gcs_backend_lists_files_via_mocked_sdk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The GCS backend converts SDK blobs to FileRefs sorted by cursor key.

    A fake ``google.cloud.storage`` module is installed before the backend's
    lazy import runs, so the test exercises the real branch logic with no
    live network call.
    """
    from datetime import UTC, datetime
    from types import ModuleType, SimpleNamespace

    from dtex.sources.filesystem.backends import GcsBackend

    blob_a = SimpleNamespace(
        name="exports/a.csv",
        updated=datetime(2026, 1, 1, tzinfo=UTC),
        size=10,
    )
    blob_b = SimpleNamespace(
        name="exports/b.csv",
        updated=datetime(2026, 2, 1, tzinfo=UTC),
        size=20,
    )

    class FakeClient:
        def bucket(self, name: str) -> Any:
            return SimpleNamespace(name=name)

        def list_blobs(self, _bucket: Any, prefix: str) -> list[Any]:
            assert prefix == "exports/"
            return [blob_a, blob_b]

    fake_storage = ModuleType("google.cloud.storage")
    fake_storage.Client = FakeClient  # type: ignore[attr-defined]
    fake_cloud = ModuleType("google.cloud")
    fake_cloud.storage = fake_storage  # type: ignore[attr-defined]
    fake_google = ModuleType("google")
    fake_google.cloud = fake_cloud  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "google", fake_google)
    monkeypatch.setitem(sys.modules, "google.cloud", fake_cloud)
    monkeypatch.setitem(sys.modules, "google.cloud.storage", fake_storage)

    backend = GcsBackend(bucket="my-bucket", prefix="exports/")
    refs = backend.list_files(
        "gs://my-bucket/exports/", "**/*.csv", cursor_strategy="mtime"
    )
    # Two blobs, sorted by their (mtime-encoded) cursor key.
    assert [r.uri for r in refs] == [
        "gs://my-bucket/exports/a.csv",
        "gs://my-bucket/exports/b.csv",
    ]
    # Cursor keys are ISO 8601 strings, lex-sorted == chronological.
    assert refs[0].cursor_key.startswith("2026-01-01")
    assert refs[1].cursor_key.startswith("2026-02-01")


def test_s3_backend_lists_files_via_mocked_sdk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The S3 backend converts paginator pages to FileRefs sorted by cursor key.

    Real branch logic exercised against a stub boto3, no AWS call.
    """
    from datetime import UTC, datetime
    from types import ModuleType

    from dtex.sources.filesystem.backends import S3Backend

    pages = [
        {
            "Contents": [
                {
                    "Key": "exports/a.csv",
                    "LastModified": datetime(2026, 3, 1, tzinfo=UTC),
                    "Size": 10,
                },
                {
                    "Key": "exports/b.csv",
                    "LastModified": datetime(2026, 4, 1, tzinfo=UTC),
                    "Size": 20,
                },
            ]
        }
    ]

    class FakePaginator:
        def paginate(self, **kwargs: Any) -> list[dict[str, Any]]:
            assert kwargs["Bucket"] == "my-bucket"
            assert kwargs["Prefix"] == "exports/"
            return pages

    class FakeClient:
        def get_paginator(self, name: str) -> FakePaginator:
            assert name == "list_objects_v2"
            return FakePaginator()

    fake_boto3 = ModuleType("boto3")
    fake_boto3.client = lambda *_a, **_k: FakeClient()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)

    backend = S3Backend(bucket="my-bucket", prefix="exports/")
    refs = backend.list_files(
        "s3://my-bucket/exports/", "**/*.csv", cursor_strategy="mtime"
    )
    assert [r.uri for r in refs] == [
        "s3://my-bucket/exports/a.csv",
        "s3://my-bucket/exports/b.csv",
    ]
    assert refs[0].cursor_key.startswith("2026-03-01")
    assert refs[1].cursor_key.startswith("2026-04-01")
