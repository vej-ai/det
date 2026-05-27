"""Behavioral tests for the dtex decorator + registration layer.

Exercises ``dtex/registry.py``: ``@stream`` / ``@resource`` registration
and callability, the full ``@destination`` hook namespace, import-time
validation of bad hook names and bad signatures, the ``Connector`` /
``@stream_method`` escape hatch, the argument-injection helper, per-connector
registration isolation, and the public ``dtex`` API surface.
"""

import inspect

import pytest

from dtex.registry import (
    DESTINATION_HOOKS,
    MANDATORY_DESTINATION_HOOKS,
    STREAM_INJECTABLES,
    ConnectorRegistry,
    DestinationHook,
    StreamRegistration,
    active_registry,
    compute_injection,
    registration_scope,
)
from dtex.types import ConnectorKind

# ---------------------------------------------------------------------------
# Public API surface
# ---------------------------------------------------------------------------


def test_public_api_decorators_importable() -> None:
    """`from dtex import stream, resource, destination, Connector` works."""
    from dtex import Connector, destination, resource, stream, stream_method

    assert callable(stream)
    assert resource is stream  # @resource is a literal alias
    assert stream_method is not None
    assert destination is not None
    assert isinstance(Connector, type)


def test_public_api_exposes_contract_types() -> None:
    """The contract types a connector author needs are re-exported from dtex."""
    import dtex

    for name in (
        "Capability",
        "Schema",
        "Field",
        "Config",
        "State",
        "Cursor",
        "Batch",
        "StateRecord",
        "WriteDisposition",
        "FieldType",
        "FieldMode",
        "CursorType",
        "SchemaContract",
        "ConnectorKind",
    ):
        assert hasattr(dtex, name), f"dtex is missing {name}"
        assert name in dtex.__all__


def test_version_still_exported() -> None:
    """__version__ survives the public-API wiring."""
    import dtex

    assert dtex.__version__ == "0.1.0"


# ---------------------------------------------------------------------------
# @stream — registration + callability
# ---------------------------------------------------------------------------


def test_stream_registers_and_stays_callable() -> None:
    """A @stream function registers in scope and remains directly callable."""
    from dtex import stream

    with registration_scope("demo_source") as reg:

        @stream(name="rates")
        def rates(config, cursor):  # type: ignore[no-untyped-def]
            yield [{"v": 1}]

        # Registered under its declared name.
        assert "rates" in reg.streams
        assert reg.kind is ConnectorKind.SOURCE
        # The wrapped function is still directly callable (unit-testing promise).
        assert list(rates(config=None, cursor=None)) == [[{"v": 1}]]

    # Declared injectables introspected from the signature.
    assert reg.stream("rates") is not None
    assert reg.stream("rates").inject == ("config", "cursor")  # type: ignore[union-attr]


def test_stream_preserves_wraps_metadata() -> None:
    """@stream preserves the wrapped function's name/docstring via functools.wraps."""
    from dtex import stream

    with registration_scope("demo"):

        @stream(name="orders")
        def orders(config):  # type: ignore[no-untyped-def]
            """Yield order batches."""
            yield []

    assert orders.__name__ == "orders"
    assert orders.__doc__ == "Yield order batches."


def test_stream_callable_outside_any_scope() -> None:
    """Outside a registration scope @stream is a no-op but the function still works."""
    from dtex import stream

    assert active_registry() is None

    @stream(name="standalone")
    def standalone(config, log):  # type: ignore[no-untyped-def]
        yield [{"ok": True}]

    # No scope was open, so nothing was registered — but the function runs and
    # self-describes via its stamped metadata.
    assert list(standalone(config=None, log=None)) == [[{"ok": True}]]
    assert standalone.__dtex_stream_name__ == "standalone"  # type: ignore[attr-defined]
    assert standalone.__dtex_inject__ == ("config", "log")  # type: ignore[attr-defined]


def test_stream_requires_name() -> None:
    """A bare @stream (no name=) raises a clear error at import time."""
    from dtex import stream

    with pytest.raises(TypeError, match="non-empty string 'name'"):

        @stream  # type: ignore[arg-type, misc]
        def no_name(config):  # type: ignore[no-untyped-def]
            yield []

    with pytest.raises(TypeError, match="non-empty string 'name'"):
        stream(name="")


def test_stream_rejects_unknown_parameter() -> None:
    """A @stream declaring a non-injectable parameter fails at decoration time."""
    from dtex import stream

    with pytest.raises(TypeError, match="widget"):

        @stream(name="bad")
        def bad(config, widget):  # type: ignore[no-untyped-def]
            yield []


def test_stream_duplicate_name_within_connector_raises() -> None:
    """Two @stream with the same name in one connector raise at import time."""
    from dtex import stream

    with pytest.raises(ValueError, match="registered twice"):
        with registration_scope("dupes"):

            @stream(name="things")
            def things_a(config):  # type: ignore[no-untyped-def]
                yield []

            @stream(name="things")
            def things_b(config):  # type: ignore[no-untyped-def]
                yield []


def test_stream_with_no_injectables_is_allowed() -> None:
    """A @stream may declare no injectables at all."""
    from dtex import stream

    with registration_scope("c") as reg:

        @stream(name="static")
        def static():  # type: ignore[no-untyped-def]
            yield [{"x": 1}]

    assert reg.stream("static").inject == ()  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# @resource — alias of @stream
# ---------------------------------------------------------------------------


def test_resource_behaves_identically_to_stream() -> None:
    """@resource registers a stream exactly like @stream — docs/03 §3.3."""
    from dtex import resource

    with registration_scope("dlt_style") as reg:

        @resource(name="customers")
        def customers(config, state):  # type: ignore[no-untyped-def]
            yield [{"id": 1}]

        assert list(customers(config=None, state=None)) == [[{"id": 1}]]

    assert reg.stream("customers") is not None
    assert reg.stream("customers").inject == ("config", "state")  # type: ignore[union-attr]
    assert reg.kind is ConnectorKind.SOURCE


# ---------------------------------------------------------------------------
# @destination — the hook namespace
# ---------------------------------------------------------------------------


def test_destination_full_hook_set_registers() -> None:
    """The full @destination hook set registers under the correct hook names."""
    from dtex import destination

    with registration_scope("bigquery") as reg:

        @destination.capabilities
        def capabilities():  # type: ignore[no-untyped-def]
            return set()

        @destination.open
        def open_conn(config):  # type: ignore[no-untyped-def]
            return {}

        @destination.ensure_schema
        def ensure_schema(conn, table, schema):  # type: ignore[no-untyped-def]
            pass

        @destination.write_batch
        def write_batch(conn, table, batch, disposition):  # type: ignore[no-untyped-def]
            return len(batch)

        @destination.commit_state
        def commit_state(conn, run_id, records):  # type: ignore[no-untyped-def]
            pass

        @destination.read_state
        def read_state(conn, connector):  # type: ignore[no-untyped-def]
            return []

        @destination.state_backend
        def state_backend(conn, config):  # type: ignore[no-untyped-def]
            return None

        @destination.transaction
        def transaction(conn, stream):  # type: ignore[no-untyped-def]
            yield

        @destination.write_run_record
        def write_run_record(conn, record):  # type: ignore[no-untyped-def]
            pass

        @destination.max_concurrent_writes
        def max_concurrent_writes(config):  # type: ignore[no-untyped-def]
            return 10

        @destination.close
        def close(conn):  # type: ignore[no-untyped-def]
            pass

    assert set(reg.hook_names) == DESTINATION_HOOKS
    assert reg.kind is ConnectorKind.DESTINATION
    # Hooks stay callable.
    assert write_batch(conn=None, table="t", batch=[1, 2, 3], disposition="append") == 3
    assert reg.missing_mandatory_hooks() == ()


def test_destination_hook_lookup_and_record() -> None:
    """A registered hook is retrievable and carries its function."""
    from dtex import destination

    with registration_scope("d") as reg:

        @destination.write_batch
        def write_batch(conn, table, batch, disposition):  # type: ignore[no-untyped-def]
            return 0

    hook = reg.hook("write_batch")
    assert isinstance(hook, DestinationHook)
    assert hook.hook == "write_batch"
    assert hook.func is write_batch


def test_destination_bad_hook_name_raises_at_import() -> None:
    """A typo'd @destination hook name raises AttributeError at import time."""
    from dtex import destination

    with pytest.raises(AttributeError, match="not a valid destination hook"):

        @destination.write_batchs  # type: ignore[misc]
        def write_batchs(conn, table, batch, disposition):  # type: ignore[no-untyped-def]
            return 0


def test_destination_hook_preserves_wraps_metadata() -> None:
    """A @destination hook preserves functools.wraps metadata."""
    from dtex import destination

    with registration_scope("d"):

        @destination.open
        def open_conn(config):  # type: ignore[no-untyped-def]
            """Open a connection."""
            return {}

    assert open_conn.__name__ == "open_conn"
    assert open_conn.__doc__ == "Open a connection."


def test_destination_duplicate_hook_raises() -> None:
    """Defining the same @destination hook twice raises at import time."""
    from dtex import destination

    with pytest.raises(ValueError, match="registered twice"):
        with registration_scope("d"):

            @destination.close
            def close_a(conn):  # type: ignore[no-untyped-def]
                pass

            @destination.close
            def close_b(conn):  # type: ignore[no-untyped-def]
                pass


def test_destination_missing_mandatory_hooks_reported() -> None:
    """missing_mandatory_hooks lists the unconditionally-required hooks not defined."""
    from dtex import destination

    with registration_scope("partial") as reg:

        @destination.open
        def open_conn(config):  # type: ignore[no-untyped-def]
            return {}

    missing = reg.missing_mandatory_hooks()
    assert set(missing) == MANDATORY_DESTINATION_HOOKS - {"open"}


def test_mixing_stream_and_destination_raises() -> None:
    """A connector cannot register both @stream and @destination — docs/03 §2.1."""
    from dtex import destination, stream

    with pytest.raises(TypeError, match="source or a destination"):
        with registration_scope("confused"):

            @stream(name="s")
            def s(config):  # type: ignore[no-untyped-def]
                yield []

            @destination.open
            def open_conn(config):  # type: ignore[no-untyped-def]
                return {}


# ---------------------------------------------------------------------------
# Connector + @stream_method — the class escape hatch
# ---------------------------------------------------------------------------


def test_connector_subclass_and_stream_method() -> None:
    """A Connector subclass registers its @stream_method streams and stays usable."""
    from dtex import Connector, stream_method

    with registration_scope("complex_erp") as reg:

        class ERPSource(Connector):
            def setup(self):  # type: ignore[no-untyped-def]
                self.opened = True

            @stream_method(name="invoices")
            def invoices(self, state, cursor):  # type: ignore[no-untyped-def]
                yield [{"id": "inv-1"}]

            @stream_method(name="customers")
            def customers(self, log):  # type: ignore[no-untyped-def]
                yield [{"id": "cust-1"}]

    # Both stream methods registered.
    assert set(reg.stream_names) == {"invoices", "customers"}
    assert reg.kind is ConnectorKind.SOURCE
    assert reg.stream("invoices").is_method is True  # type: ignore[union-attr]
    # @stream_method injectables exclude config (use self.config).
    assert reg.stream("invoices").inject == ("state", "cursor")  # type: ignore[union-attr]
    assert reg.stream("customers").inject == ("log",)  # type: ignore[union-attr]
    # The class itself is recorded for setup/teardown.
    assert ERPSource in reg.connector_classes

    # The class still works as an ordinary object: methods are callable.
    inst = ERPSource()
    inst.setup()
    assert inst.opened is True
    assert list(inst.invoices(state=None, cursor=None)) == [[{"id": "inv-1"}]]


def test_stream_method_rejects_config_param() -> None:
    """@stream_method must not declare 'config' — docs/03 §4 (use self.config)."""
    from dtex import stream_method

    with pytest.raises(TypeError, match="config"):

        class Bad:  # noqa: B903
            @stream_method(name="x")
            def x(self, config):  # type: ignore[no-untyped-def]
                yield []


def test_connector_base_setup_teardown_are_optional_noops() -> None:
    """The Connector base setup/teardown default to harmless no-ops."""
    from dtex import Connector

    with registration_scope("plain"):

        class Plain(Connector):
            pass

    inst = Plain()
    # Neither raises; both return None.
    assert inst.setup() is None
    assert inst.teardown() is None


def test_connector_defined_outside_scope_is_noop() -> None:
    """A Connector subclass defined outside a scope registers nothing but works."""
    from dtex import Connector, stream_method

    assert active_registry() is None

    class OffScope(Connector):
        @stream_method(name="thing")
        def thing(self, state):  # type: ignore[no-untyped-def]
            yield [{"n": 1}]

    inst = OffScope()
    assert list(inst.thing(state=None)) == [[{"n": 1}]]


# ---------------------------------------------------------------------------
# Argument injection helper
# ---------------------------------------------------------------------------


def test_compute_injection_picks_declared_kwargs() -> None:
    """compute_injection returns exactly the injectables the function declares."""
    from dtex import stream

    @stream(name="picky")
    def picky(config, cursor):  # type: ignore[no-untyped-def]
        yield []

    available = {"config": "CFG", "state": "ST", "cursor": "CUR", "log": "LOG"}
    kwargs = compute_injection(picky, available)
    assert kwargs == {"config": "CFG", "cursor": "CUR"}
    # state / log were available but not declared — not injected.
    assert "state" not in kwargs
    assert "log" not in kwargs


def test_compute_injection_empty_for_no_param_function() -> None:
    """A stream declaring no injectables gets an empty kwargs dict."""
    from dtex import stream

    @stream(name="none")
    def none():  # type: ignore[no-untyped-def]
        yield []

    assert compute_injection(none, {"config": 1, "state": 2}) == {}


def test_compute_injection_missing_injectable_raises() -> None:
    """A declared injectable absent from the available dict raises a clear error."""
    from dtex import stream

    @stream(name="needs_cursor")
    def needs_cursor(config, cursor):  # type: ignore[no-untyped-def]
        yield []

    # cursor is declared but the engine offered only config.
    with pytest.raises(KeyError, match="cursor"):
        compute_injection(needs_cursor, {"config": "CFG"})


def test_compute_injection_rejects_undecorated_function() -> None:
    """compute_injection on a plain function (not a @stream) raises a clear error."""

    def plain(config):  # type: ignore[no-untyped-def]
        yield []

    with pytest.raises(TypeError, match="not a dtex stream function"):
        compute_injection(plain, {"config": 1})


def test_compute_injection_works_for_stream_method() -> None:
    """compute_injection works for @stream_method functions too (config excluded)."""
    from dtex import Connector, stream_method

    with registration_scope("m") as reg:

        class M(Connector):
            @stream_method(name="rows")
            def rows(self, state, log):  # type: ignore[no-untyped-def]
                yield []

    func = reg.stream("rows").func  # type: ignore[union-attr]
    kwargs = compute_injection(func, {"state": "ST", "cursor": "CUR", "log": "LOG"})
    assert kwargs == {"state": "ST", "log": "LOG"}


def test_stream_rejects_var_args() -> None:
    """A @stream using *args/**kwargs is rejected — not by-name injectable."""
    from dtex import stream

    with pytest.raises(TypeError, match=r"\*"):

        @stream(name="splat")
        def splat(config, **kwargs):  # type: ignore[no-untyped-def]
            yield []


# ---------------------------------------------------------------------------
# Per-connector isolation
# ---------------------------------------------------------------------------


def test_two_connectors_registrations_do_not_collide() -> None:
    """Identically named streams in two connectors land in separate registries."""
    from dtex import stream

    with registration_scope("connector_a") as reg_a:

        @stream(name="orders")
        def orders_a(config):  # type: ignore[no-untyped-def]
            yield [{"src": "a"}]

    with registration_scope("connector_b") as reg_b:

        @stream(name="orders")
        def orders_b(config):  # type: ignore[no-untyped-def]
            yield [{"src": "b"}]

    # Same stream name, no collision — two distinct registries.
    assert reg_a is not reg_b
    assert reg_a.connector == "connector_a"
    assert reg_b.connector == "connector_b"
    assert reg_a.stream("orders").func is orders_a  # type: ignore[union-attr]
    assert reg_b.stream("orders").func is orders_b  # type: ignore[union-attr]
    assert list(reg_a.stream("orders").func(config=None)) == [[{"src": "a"}]]  # type: ignore[union-attr]
    assert list(reg_b.stream("orders").func(config=None)) == [[{"src": "b"}]]  # type: ignore[union-attr]


def test_scope_isolation_resets_after_exit() -> None:
    """active_registry is None before, set during, and None after a scope."""
    assert active_registry() is None
    with registration_scope("scoped") as reg:
        assert active_registry() is reg
    assert active_registry() is None


def test_multiple_files_one_connector_share_registry() -> None:
    """Decorators run at different points in one scope share the same registry."""
    from dtex import stream

    with registration_scope("multi_file") as reg:
        # Simulates source.py being imported.
        @stream(name="stream_one")
        def stream_one(config):  # type: ignore[no-untyped-def]
            yield []

        # Simulates streams.py being imported into the same connector.
        @stream(name="stream_two")
        def stream_two(config):  # type: ignore[no-untyped-def]
            yield []

    assert set(reg.stream_names) == {"stream_one", "stream_two"}


# ---------------------------------------------------------------------------
# Registry data structure
# ---------------------------------------------------------------------------


def test_empty_registry_has_no_kind() -> None:
    """A registry with no registrations has kind None."""
    reg = ConnectorRegistry(connector="empty")
    assert reg.kind is None
    assert reg.stream_names == ()
    assert reg.hook_names == ()


def test_constants_are_consistent() -> None:
    """The exported constant sets are internally consistent."""
    assert MANDATORY_DESTINATION_HOOKS <= DESTINATION_HOOKS
    assert STREAM_INJECTABLES == frozenset({"config", "state", "cursor", "log"})
    # Eleven hooks as of stage 8e (max_concurrent_writes added).
    assert len(DESTINATION_HOOKS) == 11
    assert "transaction" in DESTINATION_HOOKS
    assert "write_run_record" in DESTINATION_HOOKS
    assert "max_concurrent_writes" in DESTINATION_HOOKS


def test_stream_registration_is_frozen() -> None:
    """StreamRegistration is an immutable record."""
    from dataclasses import FrozenInstanceError

    reg = StreamRegistration(name="s", func=lambda: None, inject=())
    with pytest.raises(FrozenInstanceError):
        reg.name = "other"  # type: ignore[misc]


def test_decorated_function_signature_is_inspectable() -> None:
    """functools.wraps keeps the wrapped function's signature inspectable."""
    from dtex import stream

    @stream(name="sig")
    def sig(config, state, cursor, log):  # type: ignore[no-untyped-def]
        yield []

    params = list(inspect.signature(sig).parameters)
    assert params == ["config", "state", "cursor", "log"]
