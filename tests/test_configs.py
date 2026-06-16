"""Tests for ``dtex.engine.configs`` — the configs/*.yml parser (docs/12).

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

from dtex.engine import configs as cfgs
from dtex.engine.config import ConfigError
from dtex.types import PipelineConfig

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
            streams: all
            """
        )
    )
    found = cfgs.discover_configs(tmp_path)
    assert set(found) == {"one_dev"}
    assert found["one_dev"].source == "my_src"
    assert found["one_dev"].destination == "duckdb"
    assert found["one_dev"].target == "dev"


def test_single_config_with_params_and_explicit_streams(tmp_path: Path) -> None:
    """A single config carries params, destination_params, and explicit streams."""
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
            streams:
              items:
              events:
            schedule: "0 */6 * * *"
            """
        )
    )
    pc = cfgs.load_config("rich_dev", tmp_path)
    assert pc.params == {"page_size": 50, "start_date": "2024-01-01"}
    assert pc.destination_params == {"dataset": "my_dataset"}
    assert set(pc.streams) == {"items", "events"}
    assert pc.all_streams is False
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
            streams: all
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
                streams: all
              - name: prod_run
                source: src
                destination: duckdb
                target: prod
                streams: all
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
            streams: all
            configs:
              - name: other
                source: s
                destination: d
                streams: all
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
        "name: a\nsource: s\ndestination: d\nstreams: all\n"
    )
    (tmp_path / "configs" / "b.yml").write_text(
        "name: b\nsource: s\ndestination: d\nstreams: all\n"
    )
    found = cfgs.discover_configs(tmp_path)
    assert set(found) == {"a", "b"}


def test_duplicate_names_across_files_rejected(tmp_path: Path) -> None:
    """A config name reused across files fails discovery with a clear message."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "first.yml").write_text(
        "name: dup\nsource: s\ndestination: d\nstreams: all\n"
    )
    (tmp_path / "configs" / "second.yml").write_text(
        "name: dup\nsource: s\ndestination: d\nstreams: all\n"
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
                streams: all
              - name: same
                source: s
                destination: d
                streams: all
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
        "name: r\nsource: s\ndestination: d\nstreams: all\n"
    )
    assert set(cfgs.discover_configs(tmp_path)) == {"r"}


# --------------------------------------------------------------------------
# Validation errors
# --------------------------------------------------------------------------


def test_missing_required_field_reported_with_file_path(tmp_path: Path) -> None:
    """A required field missing fails with both the file path and the field."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "bad.yml").write_text(
        "name: c\nsource: s\nstreams: all\n"  # missing destination
    )
    with pytest.raises(ConfigError, match="destination"):
        cfgs.discover_configs(tmp_path)


def test_unknown_top_level_key_rejected(tmp_path: Path) -> None:
    """An unknown top-level key catches typos like `destintion`."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "typo.yml").write_text(
        "name: c\nsource: s\ndestintion: d\nstreams: all\n"
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
        "name: p_dev\nsource: s\ndestination: d\nstreams: all\n"
    )
    pc = cfgs.load_config("p_dev", tmp_path)
    assert isinstance(pc, PipelineConfig)
    assert pc.name == "p_dev"


def test_load_config_unknown_name_lists_known(tmp_path: Path) -> None:
    """An unknown config name fails listing the configs the project does define."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "a.yml").write_text(
        "name: a\nsource: s\ndestination: d\nstreams: all\n"
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
        "name: x\nsource: s\ndestination: d\nstreams: all\n"
    )
    found = cfgs.discover_configs(tmp_path, ["pipelines"])
    assert "x" in found


# ==========================================================================
# streams — the mandatory per-stream run-shape block (docs/12 §2-§3)
# ==========================================================================


def test_streams_all_sentinel_sets_all_streams_true(tmp_path: Path) -> None:
    """`streams: all` is the explicit catch-all opt-in (docs/12 §2.3)."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        "name: p\nsource: s\ndestination: d\nstreams: all\n"
    )
    pc = cfgs.load_config("p", tmp_path)
    assert pc.all_streams is True
    assert pc.streams == {}


def test_streams_star_sentinel_also_accepted(tmp_path: Path) -> None:
    """`streams: \"*\"` is a synonym for `streams: all`."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        'name: p\nsource: s\ndestination: d\nstreams: "*"\n'
    )
    pc = cfgs.load_config("p", tmp_path)
    assert pc.all_streams is True


def test_streams_sentinel_case_insensitive(tmp_path: Path) -> None:
    """`streams: ALL` and whitespace variants are normalized."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        "name: p\nsource: s\ndestination: d\nstreams: '  ALL  '\n"
    )
    pc = cfgs.load_config("p", tmp_path)
    assert pc.all_streams is True


def test_streams_explicit_mapping_minimal(tmp_path: Path) -> None:
    """Per-stream entries with `null` (no value) parse into default StreamRunConfig."""
    from dtex.types import StreamRunConfig

    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        textwrap.dedent(
            """\
            name: p
            source: s
            destination: d
            streams:
              customers:
              subscriptions:
            """
        )
    )
    pc = cfgs.load_config("p", tmp_path)
    assert pc.all_streams is False
    assert set(pc.streams) == {"customers", "subscriptions"}
    assert pc.streams["customers"] == StreamRunConfig()
    assert pc.streams["subscriptions"] == StreamRunConfig()


def test_streams_explicit_mapping_with_overrides(tmp_path: Path) -> None:
    """A mapping value carries mode / since / params / partition overrides."""
    from dtex.types import StreamMode

    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        textwrap.dedent(
            """\
            name: p
            source: stripe
            destination: bigquery
            streams:
              charges:
                mode: full_refresh
                since: "2026-01-01T00:00:00Z"
                params:
                  page_size: 100
            """
        )
    )
    pc = cfgs.load_config("p", tmp_path)
    sr = pc.streams["charges"]
    assert sr.mode is StreamMode.FULL_REFRESH
    assert sr.since == "2026-01-01T00:00:00Z"
    assert sr.params == {"page_size": 100}


def test_streams_bare_string_value_is_mode_shorthand(tmp_path: Path) -> None:
    """`my_stream: full_refresh` is shorthand for `{mode: full_refresh}`."""
    from dtex.types import StreamMode

    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        textwrap.dedent(
            """\
            name: p
            source: s
            destination: d
            streams:
              fast_stream: full_refresh
              slow_stream: incremental
            """
        )
    )
    pc = cfgs.load_config("p", tmp_path)
    assert pc.streams["fast_stream"].mode is StreamMode.FULL_REFRESH
    assert pc.streams["slow_stream"].mode is StreamMode.INCREMENTAL


def test_streams_partition_short_form(tmp_path: Path) -> None:
    """A short-form partition string is parsed via PartitionConfig.from_dict."""
    from dtex.types import PartitionConfig, PartitionType, TimeGranularity

    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        textwrap.dedent(
            """\
            name: p
            source: stripe
            destination: bigquery
            streams:
              invoices:
                partition: created
            """
        )
    )
    pc = cfgs.load_config("p", tmp_path)
    inv = pc.streams["invoices"].partition
    assert isinstance(inv, PartitionConfig)
    assert inv.type is PartitionType.TIME
    assert inv.granularity is TimeGranularity.DAY
    assert inv.field == "created"


def test_streams_partition_long_form_range(tmp_path: Path) -> None:
    """A long-form range partition under streams.<name>.partition parses correctly."""
    from dtex.types import PartitionConfig, PartitionRange, PartitionType

    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        textwrap.dedent(
            """\
            name: p
            source: stripe
            destination: bigquery
            streams:
              charges:
                partition:
                  field: created
                  type: range
                  range: {start: 0, end: 10000000000, interval: 86400}
            """
        )
    )
    pc = cfgs.load_config("p", tmp_path)
    ch = pc.streams["charges"].partition
    assert isinstance(ch, PartitionConfig)
    assert ch.type is PartitionType.RANGE
    assert ch.range == PartitionRange(start=0, end=10_000_000_000, interval=86_400)


# --------------------------------------------------------------------------
# streams — error surfaces (plan §5)
# --------------------------------------------------------------------------


def test_streams_missing_is_hard_error(tmp_path: Path) -> None:
    """A config without a `streams:` block fails with a friendly hint."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        "name: p\nsource: s\ndestination: d\n"
    )
    with pytest.raises(ConfigError, match="'streams' is required"):
        cfgs.load_config("p", tmp_path)


def test_streams_empty_mapping_rejected(tmp_path: Path) -> None:
    """`streams: {}` is rejected — the catch-all must be explicit."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        "name: p\nsource: s\ndestination: d\nstreams: {}\n"
    )
    with pytest.raises(ConfigError, match="'streams' must not be empty"):
        cfgs.load_config("p", tmp_path)


def test_streams_empty_list_rejected(tmp_path: Path) -> None:
    """`streams: []` (a list) is rejected with the must-be-mapping message."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        "name: p\nsource: s\ndestination: d\nstreams: []\n"
    )
    with pytest.raises(ConfigError, match="must be a mapping"):
        cfgs.load_config("p", tmp_path)


def test_streams_unknown_string_value_rejected(tmp_path: Path) -> None:
    """A non-sentinel string at `streams:` (e.g. `everything`) fails clearly."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        "name: p\nsource: s\ndestination: d\nstreams: everything\n"
    )
    with pytest.raises(ConfigError, match="must be 'all' or '\\*'"):
        cfgs.load_config("p", tmp_path)


def test_streams_unknown_mode_rejected(tmp_path: Path) -> None:
    """A bad `mode:` value names the stream and lists the valid modes."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        textwrap.dedent(
            """\
            name: p
            source: s
            destination: d
            streams:
              charges:
                mode: snapshot
            """
        )
    )
    with pytest.raises(ConfigError, match="streams\\['charges'\\]:.*unknown mode"):
        cfgs.load_config("p", tmp_path)


def test_streams_unknown_subkey_rejected(tmp_path: Path) -> None:
    """An unknown sub-key under a stream entry catches typos like `mod`."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        textwrap.dedent(
            """\
            name: p
            source: s
            destination: d
            streams:
              charges:
                mod: full_refresh
            """
        )
    )
    with pytest.raises(ConfigError, match="streams\\['charges'\\]: unknown key"):
        cfgs.load_config("p", tmp_path)


def test_streams_params_must_be_mapping(tmp_path: Path) -> None:
    """`streams.<name>.params` must itself be a mapping."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        textwrap.dedent(
            """\
            name: p
            source: s
            destination: d
            streams:
              charges:
                params:
                  - bad
            """
        )
    )
    with pytest.raises(ConfigError, match="streams\\['charges'\\].params must be"):
        cfgs.load_config("p", tmp_path)


def test_streams_partition_bad_type(tmp_path: Path) -> None:
    """A non-string non-mapping partition value is rejected."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        textwrap.dedent(
            """\
            name: p
            source: s
            destination: d
            streams:
              charges:
                partition: 42
            """
        )
    )
    with pytest.raises(ConfigError, match="streams\\['charges'\\].partition must be"):
        cfgs.load_config("p", tmp_path)


def test_streams_partition_bad_long_form_surfaces_stream_name(tmp_path: Path) -> None:
    """An invalid long-form partition entry names the offending stream."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        textwrap.dedent(
            """\
            name: p
            source: s
            destination: d
            streams:
              charges:
                partition:
                  field: created
                  type: range
                  # missing range block — invalid
            """
        )
    )
    with pytest.raises(ConfigError, match="streams\\['charges'\\].partition"):
        cfgs.load_config("p", tmp_path)


# --------------------------------------------------------------------------
# Legacy-key removal — `select:` and `partition_overrides:` are HARD-removed
# --------------------------------------------------------------------------


def test_legacy_select_key_rejected_with_pointer(tmp_path: Path) -> None:
    """`select:` is no longer supported; error points at the new location."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        textwrap.dedent(
            """\
            name: p
            source: s
            destination: d
            select: [a, b]
            streams: all
            """
        )
    )
    with pytest.raises(ConfigError, match="'select' is no longer supported"):
        cfgs.load_config("p", tmp_path)


def test_legacy_partition_overrides_rejected_with_pointer(tmp_path: Path) -> None:
    """`partition_overrides:` is no longer supported; error points at streams.<name>.partition."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        textwrap.dedent(
            """\
            name: p
            source: s
            destination: d
            streams: all
            partition_overrides:
              charges: created
            """
        )
    )
    with pytest.raises(
        ConfigError, match="'partition_overrides' is no longer supported"
    ):
        cfgs.load_config("p", tmp_path)


def test_unknown_top_level_key_still_caught_after_redesign(tmp_path: Path) -> None:
    """A typo at the top level is still a hard error after the streams redesign."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        textwrap.dedent(
            """\
            name: p
            source: s
            destination: d
            streams: all
            schadule: ""   # typo
            """
        )
    )
    with pytest.raises(ConfigError, match="unknown config key"):
        cfgs.load_config("p", tmp_path)


# --------------------------------------------------------------------------
# Tags — bare list of strings on a config (stage 8d)
# --------------------------------------------------------------------------


def test_tags_default_is_empty_tuple(tmp_path: Path) -> None:
    """A config without `tags:` has an empty tags tuple."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        "name: p\nsource: s\ndestination: d\nstreams: all\n"
    )
    pc = cfgs.load_config("p", tmp_path)
    assert pc.tags == ()


def test_tags_parses_list_of_strings(tmp_path: Path) -> None:
    """A `tags: [a, b, c]` block becomes a tuple of strings."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        textwrap.dedent(
            """\
            name: p
            source: s
            destination: d
            streams: all
            tags: [hourly, analytics, production]
            """
        )
    )
    pc = cfgs.load_config("p", tmp_path)
    assert pc.tags == ("hourly", "analytics", "production")


def test_tags_normalized_lowercase(tmp_path: Path) -> None:
    """Tags are lowercased at parse time to avoid `Hourly` vs `hourly` footguns."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        textwrap.dedent(
            """\
            name: p
            source: s
            destination: d
            streams: all
            tags: [Hourly, PROD]
            """
        )
    )
    pc = cfgs.load_config("p", tmp_path)
    assert pc.tags == ("hourly", "prod")


def test_tags_deduplicated_preserving_order(tmp_path: Path) -> None:
    """Duplicate tags are silently dedup'd; first-seen order is preserved."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        textwrap.dedent(
            """\
            name: p
            source: s
            destination: d
            streams: all
            tags: [hourly, hourly, prod, hourly]
            """
        )
    )
    pc = cfgs.load_config("p", tmp_path)
    assert pc.tags == ("hourly", "prod")


def test_tags_bare_string_is_rejected(tmp_path: Path) -> None:
    """`tags: hourly` (bare string) is a hard error — matches dbt's behavior."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        textwrap.dedent(
            """\
            name: p
            source: s
            destination: d
            streams: all
            tags: hourly
            """
        )
    )
    with pytest.raises(ConfigError, match="'tags' must be a list"):
        cfgs.load_config("p", tmp_path)


def test_tags_non_list_non_string_rejected(tmp_path: Path) -> None:
    """`tags:` declared as a mapping is also a hard error."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        textwrap.dedent(
            """\
            name: p
            source: s
            destination: d
            streams: all
            tags: {a: 1}
            """
        )
    )
    with pytest.raises(ConfigError, match="'tags' must be a list"):
        cfgs.load_config("p", tmp_path)


def test_tags_empty_entry_rejected(tmp_path: Path) -> None:
    """`tags: ['']` (an empty-string entry) is rejected."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        textwrap.dedent(
            """\
            name: p
            source: s
            destination: d
            streams: all
            tags: ['']
            """
        )
    )
    with pytest.raises(ConfigError, match="non-empty"):
        cfgs.load_config("p", tmp_path)


def test_tags_unknown_top_level_key_still_caught(tmp_path: Path) -> None:
    """Adding `tags` did not allow other typos at the top level."""
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "p.yml").write_text(
        textwrap.dedent(
            """\
            name: p
            source: s
            destination: d
            streams: all
            tags: [hourly]
            tgs: [extra]   # typo
            """
        )
    )
    with pytest.raises(ConfigError, match="unknown config key"):
        cfgs.load_config("p", tmp_path)
