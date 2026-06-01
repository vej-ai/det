"""Unit tests for the engine NORMALIZE step — :mod:`dtex.engine.normalize`.

Covers :func:`coerce_value` per :class:`~dtex.types.FieldType` and
:func:`normalize_batch` at the batch level. The end-to-end integration
(connector → engine → destination, CSV-style all-string source landing in
DuckDB) lives in :mod:`tests.test_engine`; this file is the unit harness
that catches per-rule regressions fast.
"""

from __future__ import annotations

import base64
from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from dtex.engine.normalize import coerce_value, normalize_batch
from dtex.types import CoercionError, Field, FieldMode, FieldType, Schema

# ==========================================================================
# None and empty-string short-circuits — apply uniformly to every FieldType
# ==========================================================================


@pytest.mark.parametrize(
    "field_type",
    list(FieldType),
)
def test_none_passes_through_for_every_field_type(field_type: FieldType) -> None:
    """``None`` is NULL across every FieldType — never coerced to a zero value."""
    assert coerce_value(None, field_type, column="x") is None


@pytest.mark.parametrize(
    "field_type",
    [
        FieldType.INTEGER,
        FieldType.FLOAT,
        FieldType.BOOLEAN,
        FieldType.TIMESTAMP,
        FieldType.DATE,
        FieldType.BYTES,
        FieldType.JSON,
    ],
)
def test_empty_string_becomes_none_for_non_string_types(
    field_type: FieldType,
) -> None:
    """Empty string is the CSV "no value" idiom; coerced to NULL for non-STRING."""
    assert coerce_value("", field_type, column="x") is None


def test_empty_string_is_preserved_for_string_type() -> None:
    """STRING is the one type where the empty string is a legitimate value."""
    assert coerce_value("", FieldType.STRING, column="x") == ""


# ==========================================================================
# STRING — pass through; everything else stringifies
# ==========================================================================


def test_string_passes_through_str_unchanged() -> None:
    assert coerce_value("hello", FieldType.STRING, column="x") == "hello"


def test_string_coerces_int_to_str() -> None:
    assert coerce_value(123, FieldType.STRING, column="x") == "123"


def test_string_coerces_bool_to_str() -> None:
    assert coerce_value(True, FieldType.STRING, column="x") == "True"


def test_string_coerces_float_to_str() -> None:
    assert coerce_value(1.5, FieldType.STRING, column="x") == "1.5"


# ==========================================================================
# INTEGER — int passthrough + digit-string + .0 float
# ==========================================================================


def test_integer_passes_through_int_unchanged() -> None:
    assert coerce_value(42, FieldType.INTEGER, column="x") == 42


def test_integer_accepts_digit_string() -> None:
    assert coerce_value("1599", FieldType.INTEGER, column="x") == 1599


def test_integer_accepts_negative_digit_string() -> None:
    assert coerce_value("-5", FieldType.INTEGER, column="x") == -5


def test_integer_accepts_explicit_plus_digit_string() -> None:
    assert coerce_value("+7", FieldType.INTEGER, column="x") == 7


def test_integer_accepts_whitespace_padded_digit_string() -> None:
    assert coerce_value("  42 ", FieldType.INTEGER, column="x") == 42


def test_integer_accepts_dot_zero_float() -> None:
    assert coerce_value(1.0, FieldType.INTEGER, column="x") == 1


def test_integer_accepts_dot_zero_float_string() -> None:
    assert coerce_value("3.0", FieldType.INTEGER, column="x") == 3


def test_integer_rejects_bool_true() -> None:
    """isinstance(True, int) is True — but bool must NOT silently demote to 1."""
    with pytest.raises(CoercionError) as exc:
        coerce_value(True, FieldType.INTEGER, column="flag")
    assert "flag" in str(exc.value)
    assert "True" in str(exc.value)
    assert "bool" in str(exc.value)
    assert "INTEGER" in str(exc.value)


def test_integer_rejects_non_integer_float() -> None:
    with pytest.raises(CoercionError, match="INTEGER"):
        coerce_value(1.5, FieldType.INTEGER, column="x")


def test_integer_rejects_non_numeric_string() -> None:
    with pytest.raises(CoercionError, match="INTEGER") as exc:
        coerce_value("abc", FieldType.INTEGER, column="amount")
    assert "amount" in str(exc.value)
    assert "'abc'" in str(exc.value)


def test_integer_rejects_unparseable_decimal_string() -> None:
    with pytest.raises(CoercionError, match="INTEGER"):
        coerce_value("1.5", FieldType.INTEGER, column="x")


def test_integer_rejects_list() -> None:
    with pytest.raises(CoercionError, match="INTEGER"):
        coerce_value([1, 2], FieldType.INTEGER, column="x")


# ==========================================================================
# FLOAT — int/float passthrough + parseable string + scientific notation
# ==========================================================================


def test_float_passes_through_float_unchanged() -> None:
    assert coerce_value(1.5, FieldType.FLOAT, column="x") == 1.5


def test_float_accepts_int() -> None:
    assert coerce_value(2, FieldType.FLOAT, column="x") == 2.0
    assert isinstance(coerce_value(2, FieldType.FLOAT, column="x"), float)


def test_float_accepts_parseable_string() -> None:
    assert coerce_value("3.14", FieldType.FLOAT, column="x") == 3.14


def test_float_accepts_scientific_notation_string() -> None:
    assert coerce_value("1.5e3", FieldType.FLOAT, column="x") == 1500.0


def test_float_accepts_integer_string() -> None:
    assert coerce_value("42", FieldType.FLOAT, column="x") == 42.0


def test_float_rejects_bool() -> None:
    with pytest.raises(CoercionError, match="FLOAT"):
        coerce_value(False, FieldType.FLOAT, column="x")


def test_float_rejects_garbage_string() -> None:
    with pytest.raises(CoercionError, match="FLOAT"):
        coerce_value("not-a-number", FieldType.FLOAT, column="x")


# ==========================================================================
# BOOLEAN — bool / 0-1 int / true-false-yes-no-1-0 string
# ==========================================================================


def test_boolean_passes_through_true() -> None:
    assert coerce_value(True, FieldType.BOOLEAN, column="x") is True


def test_boolean_passes_through_false() -> None:
    assert coerce_value(False, FieldType.BOOLEAN, column="x") is False


@pytest.mark.parametrize("s", ["true", "True", "TRUE", "TruE", "1", "yes", "YES"])
def test_boolean_accepts_true_strings(s: str) -> None:
    assert coerce_value(s, FieldType.BOOLEAN, column="x") is True


@pytest.mark.parametrize("s", ["false", "False", "FALSE", "0", "no", "NO"])
def test_boolean_accepts_false_strings(s: str) -> None:
    assert coerce_value(s, FieldType.BOOLEAN, column="x") is False


def test_boolean_accepts_int_zero() -> None:
    assert coerce_value(0, FieldType.BOOLEAN, column="x") is False


def test_boolean_accepts_int_one() -> None:
    assert coerce_value(1, FieldType.BOOLEAN, column="x") is True


def test_boolean_rejects_other_ints() -> None:
    with pytest.raises(CoercionError, match="BOOLEAN"):
        coerce_value(2, FieldType.BOOLEAN, column="x")


def test_boolean_rejects_unknown_string() -> None:
    with pytest.raises(CoercionError, match="BOOLEAN") as exc:
        coerce_value("maybe", FieldType.BOOLEAN, column="active")
    assert "active" in str(exc.value)
    assert "'maybe'" in str(exc.value)


def test_boolean_rejects_float() -> None:
    with pytest.raises(CoercionError, match="BOOLEAN"):
        coerce_value(1.0, FieldType.BOOLEAN, column="x")


# ==========================================================================
# TIMESTAMP — datetime / ISO-8601 / Unix-epoch (int/float/digit-string)
# ==========================================================================


def test_timestamp_passes_through_aware_datetime_in_utc() -> None:
    dt = datetime(2026, 5, 31, 12, 0, tzinfo=UTC)
    assert coerce_value(dt, FieldType.TIMESTAMP, column="x") == dt


def test_timestamp_converts_aware_datetime_in_other_tz_to_utc() -> None:
    eastern = timezone(timedelta(hours=-5))
    dt = datetime(2026, 5, 31, 12, 0, tzinfo=eastern)
    coerced = coerce_value(dt, FieldType.TIMESTAMP, column="x")
    assert coerced.tzinfo is UTC
    assert coerced.hour == 17  # 12 EST → 17 UTC


def test_timestamp_stamps_naive_datetime_as_utc() -> None:
    dt = datetime(2026, 5, 31, 12, 0)
    coerced = coerce_value(dt, FieldType.TIMESTAMP, column="x")
    assert coerced.tzinfo is UTC
    assert coerced.hour == 12


def test_timestamp_accepts_iso8601_with_t_and_z() -> None:
    coerced = coerce_value("2026-05-31T12:00:00Z", FieldType.TIMESTAMP, column="x")
    assert coerced == datetime(2026, 5, 31, 12, 0, tzinfo=UTC)


def test_timestamp_accepts_iso8601_with_offset() -> None:
    coerced = coerce_value(
        "2026-05-31T12:00:00+02:00", FieldType.TIMESTAMP, column="x"
    )
    assert coerced.tzinfo is UTC
    assert coerced.hour == 10  # +02:00 → UTC


def test_timestamp_accepts_iso8601_with_space_separator() -> None:
    coerced = coerce_value("2026-05-31 12:00:00", FieldType.TIMESTAMP, column="x")
    assert coerced == datetime(2026, 5, 31, 12, 0, tzinfo=UTC)


def test_timestamp_accepts_iso8601_with_fractional_seconds() -> None:
    coerced = coerce_value(
        "2026-05-31T12:00:00.123456Z", FieldType.TIMESTAMP, column="x"
    )
    assert coerced.microsecond == 123456


def test_timestamp_accepts_unix_epoch_int() -> None:
    # 2021-01-01 00:00:00 UTC = 1609459200
    coerced = coerce_value(1609459200, FieldType.TIMESTAMP, column="x")
    assert coerced == datetime(2021, 1, 1, tzinfo=UTC)


def test_timestamp_accepts_unix_epoch_float() -> None:
    coerced = coerce_value(1609459200.5, FieldType.TIMESTAMP, column="x")
    assert coerced.microsecond == 500000


def test_timestamp_accepts_unix_epoch_digit_string() -> None:
    coerced = coerce_value("1609459200", FieldType.TIMESTAMP, column="x")
    assert coerced == datetime(2021, 1, 1, tzinfo=UTC)


def test_timestamp_accepts_date_as_midnight_utc() -> None:
    coerced = coerce_value(date(2026, 5, 31), FieldType.TIMESTAMP, column="x")
    assert coerced == datetime(2026, 5, 31, tzinfo=UTC)


def test_timestamp_rejects_bool() -> None:
    with pytest.raises(CoercionError, match="TIMESTAMP"):
        coerce_value(True, FieldType.TIMESTAMP, column="x")


def test_timestamp_rejects_garbage_string() -> None:
    with pytest.raises(CoercionError, match="TIMESTAMP") as exc:
        coerce_value("not-a-date", FieldType.TIMESTAMP, column="created")
    assert "created" in str(exc.value)


def test_timestamp_rejects_list() -> None:
    with pytest.raises(CoercionError, match="TIMESTAMP"):
        coerce_value([], FieldType.TIMESTAMP, column="x")


# ==========================================================================
# DATE — date / datetime (drop time) / YYYY-MM-DD
# ==========================================================================


def test_date_passes_through_date_unchanged() -> None:
    d = date(2026, 5, 31)
    assert coerce_value(d, FieldType.DATE, column="x") == d


def test_date_drops_time_from_datetime() -> None:
    dt = datetime(2026, 5, 31, 12, 30)
    assert coerce_value(dt, FieldType.DATE, column="x") == date(2026, 5, 31)


def test_date_accepts_aware_datetime_uses_utc_day() -> None:
    # 03:00 EST = 08:00 UTC, same calendar day; this checks tz-aware path.
    eastern = timezone(timedelta(hours=-5))
    dt = datetime(2026, 5, 31, 3, 0, tzinfo=eastern)
    assert coerce_value(dt, FieldType.DATE, column="x") == date(2026, 5, 31)


def test_date_accepts_iso_string() -> None:
    assert coerce_value("2026-05-31", FieldType.DATE, column="x") == date(
        2026, 5, 31
    )


def test_date_rejects_garbage_string() -> None:
    with pytest.raises(CoercionError, match="DATE"):
        coerce_value("not-a-date", FieldType.DATE, column="x")


def test_date_rejects_int() -> None:
    with pytest.raises(CoercionError, match="DATE"):
        coerce_value(20260531, FieldType.DATE, column="x")


# ==========================================================================
# JSON — pass-through for dict/list/str/scalar
# ==========================================================================


def test_json_passes_through_dict() -> None:
    payload = {"key": "value", "nested": [1, 2]}
    assert coerce_value(payload, FieldType.JSON, column="x") == payload


def test_json_passes_through_list() -> None:
    payload = [1, 2, 3]
    assert coerce_value(payload, FieldType.JSON, column="x") == payload


def test_json_passes_through_string_unchanged() -> None:
    # JSON-text already; destination's _encode_json_column handles.
    assert coerce_value('{"a":1}', FieldType.JSON, column="x") == '{"a":1}'


def test_json_passes_through_scalar() -> None:
    assert coerce_value(42, FieldType.JSON, column="x") == 42


# ==========================================================================
# BYTES — bytes / base64 string / raw string utf-8
# ==========================================================================


def test_bytes_passes_through_bytes_unchanged() -> None:
    assert coerce_value(b"hello", FieldType.BYTES, column="x") == b"hello"


def test_bytes_decodes_base64_string() -> None:
    encoded = base64.b64encode(b"hello world").decode("ascii")
    assert coerce_value(encoded, FieldType.BYTES, column="x") == b"hello world"


def test_bytes_encodes_plain_text_as_utf8() -> None:
    # "hello" is 5 chars — fails the base64 length-multiple-of-4 gate.
    assert coerce_value("hello", FieldType.BYTES, column="x") == b"hello"


def test_bytes_encodes_unicode_as_utf8() -> None:
    # Non-ASCII text definitely doesn't pass the base64 alphabet gate.
    assert coerce_value("héllo", FieldType.BYTES, column="x") == "héllo".encode()


def test_bytes_rejects_int() -> None:
    with pytest.raises(CoercionError, match="BYTES"):
        coerce_value(42, FieldType.BYTES, column="x")


# ==========================================================================
# CoercionError message formatting + structured fields
# ==========================================================================


def test_coercion_error_message_format() -> None:
    """The message names column, repr-quoted value, source type, target FieldType."""
    with pytest.raises(CoercionError) as exc:
        coerce_value("abc", FieldType.INTEGER, column="amount")
    msg = str(exc.value)
    assert msg == "column 'amount': could not coerce 'abc' (str) to INTEGER"


def test_coercion_error_structured_fields() -> None:
    """The exception exposes the column / value / source_type / target_type fields."""
    with pytest.raises(CoercionError) as exc:
        coerce_value("abc", FieldType.INTEGER, column="amount")
    err = exc.value
    assert err.column == "amount"
    assert err.value == "abc"
    assert err.source_type is str
    assert err.target_type is FieldType.INTEGER


def test_coercion_error_truncates_long_value() -> None:
    """A 1000-char string's repr is truncated to ~80 chars + ellipsis."""
    long_string = "x" * 1000
    with pytest.raises(CoercionError) as exc:
        coerce_value(long_string, FieldType.INTEGER, column="x")
    msg = str(exc.value)
    # Truncated form ends with `…` and the bare 1000-x repr (length 1002 including
    # quotes) cannot appear in the message in full.
    assert "…" in msg
    assert "x" * 1000 not in msg


def test_coercion_error_is_value_error_subclass() -> None:
    """CoercionError extends ValueError so existing except-ValueError handlers catch it."""
    with pytest.raises(ValueError):
        coerce_value("abc", FieldType.INTEGER, column="x")


def test_coercion_error_value_repr_is_quoted_for_visibility() -> None:
    """A whitespace-only string's truncated repr keeps the quotes visible."""
    with pytest.raises(CoercionError) as exc:
        coerce_value("   ", FieldType.INTEGER, column="x")
    # repr of the original input survives in the message — quotes make
    # the whitespace visible to an operator.
    assert "'   '" in str(exc.value)


# ==========================================================================
# normalize_batch — batch-level driver
# ==========================================================================


def _schema(*pairs: tuple[str, FieldType]) -> Schema:
    return Schema(fields=tuple(Field(name=n, type=t) for n, t in pairs))


def test_normalize_batch_empty_returns_empty_list() -> None:
    assert normalize_batch([], _schema(("id", FieldType.INTEGER))) == []


def test_normalize_batch_coerces_each_record() -> None:
    schema = _schema(
        ("id", FieldType.INTEGER),
        ("amount", FieldType.FLOAT),
        ("active", FieldType.BOOLEAN),
    )
    batch = [
        {"id": "1", "amount": "1.5", "active": "true"},
        {"id": "2", "amount": "2.0", "active": "false"},
    ]
    result = normalize_batch(batch, schema)
    assert result == [
        {"id": 1, "amount": 1.5, "active": True},
        {"id": 2, "amount": 2.0, "active": False},
    ]


def test_normalize_batch_leaves_extra_columns_unchanged() -> None:
    """Columns not in schema pass through verbatim — schema-evolution territory."""
    schema = _schema(("id", FieldType.INTEGER))
    batch = [{"id": "1", "surprise": "extra"}]
    result = normalize_batch(batch, schema)
    assert result == [{"id": 1, "surprise": "extra"}]


def test_normalize_batch_handles_missing_columns_silently() -> None:
    """A record may omit a schema column — coerce only what's present."""
    schema = _schema(
        ("id", FieldType.INTEGER), ("amount", FieldType.FLOAT)
    )
    batch = [{"id": "1"}, {"id": "2", "amount": "2.0"}]
    result = normalize_batch(batch, schema)
    assert result == [{"id": 1}, {"id": 2, "amount": 2.0}]


def test_normalize_batch_does_not_mutate_input() -> None:
    """Returned dicts are fresh — connectors that stash records aren't affected."""
    schema = _schema(("id", FieldType.INTEGER))
    original = {"id": "1"}
    batch = [original]
    normalize_batch(batch, schema)
    assert original == {"id": "1"}  # input string preserved


def test_normalize_batch_already_typed_passes_through() -> None:
    """Zero behavior change for records that already match the declared types."""
    schema = _schema(
        ("id", FieldType.INTEGER),
        ("amount", FieldType.FLOAT),
        ("active", FieldType.BOOLEAN),
    )
    batch = [{"id": 1, "amount": 1.5, "active": True}]
    result = normalize_batch(batch, schema)
    assert result == batch
    # Same data, but a NEW dict object.
    assert result[0] is not batch[0]


def test_normalize_batch_empty_schema_returns_shallow_copies() -> None:
    """No declared types → no coercion; uniform fresh-dict contract upheld."""
    schema = Schema(fields=())
    original = {"a": 1, "b": "x"}
    batch = [original]
    result = normalize_batch(batch, schema)
    assert result == batch
    assert result[0] is not original


def test_normalize_batch_raises_with_record_failed_column() -> None:
    """A bad value in batch 2 raises CoercionError naming the column."""
    schema = _schema(("amount", FieldType.INTEGER))
    batch = [{"amount": "1"}, {"amount": "oops"}]
    with pytest.raises(CoercionError) as exc:
        normalize_batch(batch, schema)
    assert "amount" in str(exc.value)
    assert "INTEGER" in str(exc.value)


def test_normalize_batch_required_field_can_still_be_none() -> None:
    """A schema's NULLABLE/REQUIRED mode is not enforced by NORMALIZE.

    The destination's ensure_schema is the place where REQUIRED becomes
    a runtime constraint; NORMALIZE only coerces the value's TYPE. A
    record explicitly carrying None for a REQUIRED-mode column flows
    through coerce_value unchanged and lets the destination reject it
    (or accept it, in the rare case the destination doesn't enforce
    NOT NULL).
    """
    schema = Schema(
        fields=(
            Field(name="id", type=FieldType.INTEGER, mode=FieldMode.REQUIRED),
        )
    )
    batch = [{"id": None}]
    result = normalize_batch(batch, schema)
    assert result == [{"id": None}]
