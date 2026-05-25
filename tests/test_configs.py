"""Tests for ``det.engine.configs`` — the configs/*.yml parser (docs/12).

Stage 8.B introduced ``configs/`` as the runtime unit. This module focuses on
the parser's contracts: the two accepted file shapes, the error paths, the
discover-vs-load interplay, and the override semantics around target
selection.

The lifecycle assertions for a config-driven run live in ``test_engine.py``
and ``test_smoke.py``; this file is the unit-test layer for the parser
itself.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from det.engine import configs as cfgs
from det.engine.config import ConfigError
from det.types import PipelineConfig

# --------------------------------------------------------------------------
# Single-config file shape
# --------------------------------------------------------------------------


def test_single_config_file_parses(tmp_path: Path) -> None:
    """A file with top-level `name`/`source`/`destination` parses as one config."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "one.yml").write_text(
        textwrap.dedent(
            """\
            name: one_dev
            source: my_src
            destination: duckdb
            target: dev
            """
        )
    )
    found = cfgs.discover_configs(tmp_path)
    assert set(found) == {"one_dev"}
    assert found["one_dev"].source == "my_src"
    assert found["one_dev"].destination == "duckdb"
    assert found["one_dev"].target == "dev"


def test_single_config_with_params_and_select(tmp_path: Path) -> None:
    """A single config carries params, destination_params, and select."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "rich.yml").write_text(
        textwrap.dedent(
            """\
            name: rich_dev
            source: api
            destination: warehouse
            params:
              page_size: 50
              start_date: "2024-01-01"
            destination_params:
              dataset: my_dataset
            select:
              - items
              - events
            schedule: "0 */6 * * *"
            """
        )
    )
    pc = cfgs.load_config("rich_dev", tmp_path)
    assert pc.params == {"page_size": 50, "start_date": "2024-01-01"}
    assert pc.destination_params == {"dataset": "my_dataset"}
    assert pc.select == ("items", "events")
    assert pc.schedule == "0 */6 * * *"


def test_yaml_extension_also_discovered(tmp_path: Path) -> None:
    """`.yaml` is treated the same as `.yml`."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "alt.yaml").write_text(
        textwrap.dedent(
            """\
            name: alt
            source: s
            destination: d
            """
        )
    )
    assert "alt" in cfgs.discover_configs(tmp_path)


# --------------------------------------------------------------------------
# Multi-config file shape — `configs:` list
# --------------------------------------------------------------------------


def test_multi_config_file_parses(tmp_path: Path) -> None:
    """A file with a `configs:` list yields one PipelineConfig per entry."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "many.yml").write_text(
        textwrap.dedent(
            """\
            configs:
              - name: dev_run
                source: src
                destination: duckdb
                target: dev
              - name: prod_run
                source: src
                destination: duckdb
                target: prod
            """
        )
    )
    found = cfgs.discover_configs(tmp_path)
    assert set(found) == {"dev_run", "prod_run"}
    assert found["dev_run"].target == "dev"
    assert found["prod_run"].target == "prod"


def test_multi_and_single_shapes_in_one_file_rejected(tmp_path: Path) -> None:
    """A file that mixes a single-config and a `configs:` list is a hard error."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "ambiguous.yml").write_text(
        textwrap.dedent(
            """\
            name: mine
            source: s
            destination: d
            configs:
              - name: other
                source: s
                destination: d
            """
        )
    )
    with pytest.raises(ConfigError, match="mutually exclusive"):
        cfgs.discover_configs(tmp_path)


# --------------------------------------------------------------------------
# Discovery across files
# --------------------------------------------------------------------------


def test_two_files_yield_two_configs(tmp_path: Path) -> None:
    """Multiple files in configs/ are scanned and merged."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "a.yml").write_text(
        "name: a\nsource: s\ndestination: d\n"
    )
    (tmp_path / "configs" / "b.yml").write_text(
        "name: b\nsource: s\ndestination: d\n"
    )
    found = cfgs.discover_configs(tmp_path)
    assert set(found) == {"a", "b"}


def test_duplicate_names_across_files_rejected(tmp_path: Path) -> None:
    """A config name reused across files fails discovery with a clear message."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "first.yml").write_text(
        "name: dup\nsource: s\ndestination: d\n"
    )
    (tmp_path / "configs" / "second.yml").write_text(
        "name: dup\nsource: s\ndestination: d\n"
    )
    with pytest.raises(ConfigError, match="duplicate config name"):
        cfgs.discover_configs(tmp_path)


def test_duplicate_names_within_multi_file_rejected(tmp_path: Path) -> None:
    """Two entries with the same name inside one `configs:` list are also rejected."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "dups.yml").write_text(
        textwrap.dedent(
            """\
            configs:
              - name: same
                source: s
                destination: d
              - name: same
                source: s
                destination: d
            """
        )
    )
    with pytest.raises(ConfigError, match="duplicate config name"):
        cfgs.discover_configs(tmp_path)


def test_empty_configs_dir_returns_empty(tmp_path: Path) -> None:
    """An empty configs/ directory yields no configs (no error)."""
    (tmp_path / "configs").mkdir()
    assert cfgs.discover_configs(tmp_path) == {}


def test_absent_configs_dir_returns_empty(tmp_path: Path) -> None:
    """A project with no configs/ directory yields no configs (no error)."""
    assert cfgs.discover_configs(tmp_path) == {}


def test_empty_file_yields_no_configs(tmp_path: Path) -> None:
    """A blank YAML file does not contribute and does not crash."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "blank.yml").write_text("")
    (tmp_path / "configs" / "real.yml").write_text(
        "name: r\nsource: s\ndestination: d\n"
    )
    assert set(cfgs.discover_configs(tmp_path)) == {"r"}


# --------------------------------------------------------------------------
# Validation errors
# --------------------------------------------------------------------------


def test_missing_required_field_reported_with_file_path(tmp_path: Path) -> None:
    """A required field missing fails with both the file path and the field."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "bad.yml").write_text(
        "name: c\nsource: s\n"  # missing destination
    )
    with pytest.raises(ConfigError, match="destination"):
        cfgs.discover_configs(tmp_path)


def test_unknown_top_level_key_rejected(tmp_path: Path) -> None:
    """An unknown top-level key catches typos like `destintion`."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "typo.yml").write_text(
        "name: c\nsource: s\ndestintion: d\n"
    )
    with pytest.raises(ConfigError, match="unknown config key"):
        cfgs.discover_configs(tmp_path)


def test_invalid_yaml_reports_file(tmp_path: Path) -> None:
    """An unparseable YAML file fails with the file path in the message."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "broken.yml").write_text("this: : : :\n")
    with pytest.raises(ConfigError, match="broken.yml"):
        cfgs.discover_configs(tmp_path)


def test_non_mapping_file_rejected(tmp_path: Path) -> None:
    """A file that parses to a list (not a mapping) is a hard error."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "list.yml").write_text("- 1\n- 2\n")
    with pytest.raises(ConfigError, match="must parse to a mapping"):
        cfgs.discover_configs(tmp_path)


# --------------------------------------------------------------------------
# load_config — lookup by name
# --------------------------------------------------------------------------


def test_load_config_finds_existing(tmp_path: Path) -> None:
    """load_config returns the named PipelineConfig."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        "name: p_dev\nsource: s\ndestination: d\n"
    )
    pc = cfgs.load_config("p_dev", tmp_path)
    assert isinstance(pc, PipelineConfig)
    assert pc.name == "p_dev"


def test_load_config_unknown_name_lists_known(tmp_path: Path) -> None:
    """An unknown config name fails listing the configs the project does define."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "a.yml").write_text(
        "name: a\nsource: s\ndestination: d\n"
    )
    with pytest.raises(ConfigError, match="known configs.*a"):
        cfgs.load_config("missing", tmp_path)


def test_load_config_no_configs_dir_reports_empty(tmp_path: Path) -> None:
    """load_config on a project with no configs/ directory reports `(none defined)`."""
    with pytest.raises(ConfigError, match="none defined"):
        cfgs.load_config("anything", tmp_path)


# --------------------------------------------------------------------------
# Custom config_paths
# --------------------------------------------------------------------------


def test_custom_config_paths(tmp_path: Path) -> None:
    """A non-default config_paths list is honored."""
    (tmp_path / "pipelines").mkdir()
    (tmp_path / "pipelines" / "x.yml").write_text(
        "name: x\nsource: s\ndestination: d\n"
    )
    found = cfgs.discover_configs(tmp_path, ["pipelines"])
    assert "x" in found


# --------------------------------------------------------------------------
# partition_overrides — per-stream physical-partition overrides (docs/12, docs/05 §3.x)
# --------------------------------------------------------------------------


def test_partition_overrides_short_form_per_stream(tmp_path: Path) -> None:
    """A short-form (string) per-stream override parses into a PartitionConfig."""
    from det.types import PartitionConfig, PartitionType, TimeGranularity

    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        textwrap.dedent(
            """\
            name: p
            source: stripe
            destination: bigquery
            partition_overrides:
              invoices: created
            """
        )
    )
    pc = cfgs.load_config("p", tmp_path)
    assert "invoices" in pc.partition_overrides
    inv = pc.partition_overrides["invoices"]
    assert isinstance(inv, PartitionConfig)
    assert inv.type is PartitionType.TIME
    assert inv.granularity is TimeGranularity.DAY
    assert inv.field == "created"


def test_partition_overrides_long_form_range(tmp_path: Path) -> None:
    """A long-form range entry under partition_overrides parses correctly."""
    from det.types import PartitionRange, PartitionType

    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        textwrap.dedent(
            """\
            name: p
            source: stripe
            destination: bigquery
            partition_overrides:
              charges:
                field: created
                type: range
                range: {start: 0, end: 10000000000, interval: 86400}
            """
        )
    )
    pc = cfgs.load_config("p", tmp_path)
    ch = pc.partition_overrides["charges"]
    assert ch.type is PartitionType.RANGE
    assert ch.range == PartitionRange(start=0, end=10_000_000_000, interval=86_400)


def test_partition_overrides_mixed_short_and_long(tmp_path: Path) -> None:
    """A single partition_overrides block accepts both forms simultaneously."""
    from det.types import PartitionType

    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        textwrap.dedent(
            """\
            name: p
            source: stripe
            destination: bigquery
            partition_overrides:
              invoices: created
              charges:
                field: created
                type: range
                range: {start: 0, end: 100, interval: 10}
            """
        )
    )
    pc = cfgs.load_config("p", tmp_path)
    assert pc.partition_overrides["invoices"].type is PartitionType.TIME
    assert pc.partition_overrides["charges"].type is PartitionType.RANGE


def test_partition_overrides_must_be_mapping(tmp_path: Path) -> None:
    """A list (not mapping) under partition_overrides fails fast."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        textwrap.dedent(
            """\
            name: p
            source: stripe
            destination: bigquery
            partition_overrides:
              - charges
              - invoices
            """
        )
    )
    with pytest.raises(ConfigError, match="must be a mapping"):
        cfgs.load_config("p", tmp_path)


def test_partition_overrides_bad_long_form_surfaces_stream_name(tmp_path: Path) -> None:
    """An invalid long-form entry names the offending stream in the error."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        textwrap.dedent(
            """\
            name: p
            source: stripe
            destination: bigquery
            partition_overrides:
              charges:
                field: created
                type: range
                # missing range block — invalid
            """
        )
    )
    with pytest.raises(ConfigError, match="partition_overrides\\['charges'\\]"):
        cfgs.load_config("p", tmp_path)


def test_partition_overrides_default_is_empty(tmp_path: Path) -> None:
    """A config without partition_overrides has an empty default mapping."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        "name: p\nsource: s\ndestination: d\n"
    )
    pc = cfgs.load_config("p", tmp_path)
    assert pc.partition_overrides == {}


def test_partition_overrides_unknown_top_level_key_still_caught(tmp_path: Path) -> None:
    """Adding partition_overrides did not allow other typos at the top level."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        textwrap.dedent(
            """\
            name: p
            source: s
            destination: d
            partition_overides: {}   # typo
            """
        )
    )
    with pytest.raises(ConfigError, match="unknown config key"):
        cfgs.load_config("p", tmp_path)
