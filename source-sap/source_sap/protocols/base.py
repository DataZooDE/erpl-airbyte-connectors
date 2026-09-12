"""The contract every SAP protocol implements.

A driver knows three things: how to prove the connection works, which objects
exist and what they look like, and how to turn one object into a list of
independently-readable *read plans*.  Everything else -- threading, record
emission, state -- lives in the generic stream/cursor layer.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import duckdb

from source_sap.session import ErplSession

logger = logging.getLogger("airbyte")


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

    def prepare(  # noqa: B027 - an optional hook, deliberately not abstract
        self, session: ErplSession, obj: SapObject, state: Mapping[str, Any]
    ) -> None:
        """Restore any client-side position before partitions are generated."""

    def warn_about_insecure_transport(self) -> None:
        """Say so when credentials will travel in the clear.

        SAP's own default is unencrypted RFC, which is fine against a local trial
        system and wrong against anything else.
        """
        if str(self.config.get("snc_mode") or "0").strip() != "1":
            logger.warning(
                "SNC is not enabled, so this SAP RFC connection is unencrypted and the "
                "password travels in the clear. Set snc_mode to '1' with an SNC library "
                "and partner name for anything other than a local test system."
            )

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


def clamp(value: Any, low: int, high: int, default: int | None = None) -> int | None:
    """Keep a tuning knob inside its documented range.

    These reach SAP as thread and package counts; a hand-edited config should not
    be able to ask for ten thousand parallel readers.
    """
    if value is None or value == "":
        return default
    try:
        return max(low, min(high, int(value)))
    except (TypeError, ValueError):
        return default


def sql_string_literal(value: object) -> str:
    """Render a value as a single-quoted SQL string literal.

    ERPL's table functions take their object names, filters and column lists as
    *constant* arguments, so those call sites cannot use a bind parameter. This
    is the only thing standing between a config value and the SQL text, so it
    escapes rather than validates -- callers that can validate against discovery
    should do so as well, but nothing here assumes they have.

    Note it produces a string *literal* (`'x'`), not a quoted identifier (`"x"`);
    ERPL takes object names as literals.
    """
    return "'" + str(value).replace("'", "''") + "'"
