"""The contract every SAP protocol implements.

A driver knows three things: how to prove the connection works, which objects
exist and what they look like, and how to turn one object into a list of
independently-readable *read plans*.  Everything else -- threading, record
emission, state -- lives in the generic stream/cursor layer.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import duckdb

from source_sap.session import ErplSession


@dataclass(frozen=True)
class ReadPlan:
    """One unit of work: a parameterised query plus the slice it represents."""

    sql: str
    params: Sequence[Any] = ()
    slice_: Mapping[str, Any] = field(default_factory=dict)

    def execute(self, cursor: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
        return cursor.execute(self.sql, list(self.params)) if self.params else cursor.execute(self.sql)


@dataclass(frozen=True)
class SapObject:
    """A discovered SAP object, ready to become an Airbyte stream."""

    name: str
    json_schema: Mapping[str, Any]
    primary_key: list[list[str]] | None = None
    supports_incremental: bool = False
    # Whether records carry a change marker and deletes must become CDC tombstones.
    change_mode_field: str | None = None
    # Driver-private payload (SAP object name, ODP context, entity-set URL, ...).
    meta: Mapping[str, Any] = field(default_factory=dict)


class ProtocolDriver(ABC):
    """Base class for the four SAP access protocols."""

    #: value of `protocol.mode` in the connector config
    mode: str = ""
    #: ERPL extensions this driver needs loaded
    required_extensions: tuple[str, ...] = ()

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.config = config
        self.options: Mapping[str, Any] = config.get("protocol") or {}

    # ---- lifecycle ------------------------------------------------------------

    @abstractmethod
    def check(self, session: ErplSession) -> str:
        """Prove the connection works. Raise AirbyteTracedException otherwise."""

    @abstractmethod
    def discover(self, session: ErplSession) -> list[SapObject]:
        """Enumerate the objects this configuration exposes."""

    @abstractmethod
    def read_plans(
        self, session: ErplSession, obj: SapObject, *, incremental: bool, state: Mapping[str, Any]
    ) -> list[ReadPlan]:
        """Split one object into independently readable plans."""

    # ---- optional hooks -------------------------------------------------------

    def on_success(  # noqa: B027 - an optional hook, deliberately not abstract
        self, session: ErplSession, obj: SapObject, state: Mapping[str, Any]
    ) -> None:
        """Called once a stream has been fully read (cursor cleanup, etc.)."""

    def next_state(self, session: ErplSession, obj: SapObject, previous: Mapping[str, Any]) -> Mapping[str, Any]:
        """State to checkpoint after a successful incremental read."""
        return dict(previous)

    def concurrency_group(self, obj: SapObject) -> str:
        """Non-empty to stop streams sharing a server-side resource running together."""
        return ""

    # ---- shared helpers -------------------------------------------------------

    def _object_overrides(self) -> dict[str, Mapping[str, Any]]:
        """Per-object overrides from the config, keyed by object name."""
        overrides: dict[str, Mapping[str, Any]] = {}
        for entry in self.options.get("objects") or []:
            key = entry.get("name") or entry.get("entity_set") or entry.get("url")
            if key:
                overrides[str(key)] = entry
        return overrides


def quote_identifier(value: str) -> str:
    """Quote a SAP object name for use where DuckDB takes no bind parameter.

    ERPL's table functions take their object name as a *constant* argument, so
    a few call sites cannot use a bind parameter.  Everything that reaches this
    function is validated against the discovery result first; this is the second
    line of defence.
    """
    return "'" + value.replace("'", "''") + "'"
