"""SAP ODP over RFC (`erpl_odp`) -- full snapshots and server-side deltas.

The delta pointer lives on the SAP server, keyed by
``(context, odp_name, subscriber_name, subscriber_process)``.  ``subscriber_name``
is chosen by ERPL, so **the only thing this connector has to persist is the
``subscriber_process`` string** -- there is no client-side token.

The consequence is that losing Airbyte state orphans an ODQ subscription that
keeps retaining delta data on the SAP system, so the name is derived
deterministically from the stream rather than generated per run.
"""

from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Mapping
from typing import Any

from source_sap.errors import config_error, traced
from source_sap.protocols.base import ProtocolDriver, ReadPlan, SapObject, quote_identifier
from source_sap.retry import retry_transient
from source_sap.session import ErplSession
from source_sap.types import json_schema_for_fields, primary_key_for_fields

logger = logging.getLogger("airbyte")

# RODPS_REPL subscriber process is CHAR(32).
_MAX_SUBSCRIBER_PROCESS = 32
_PREFIX = "AB_"

# Columns ODP adds to every record; they describe the change, not the entity.
ODP_CONTROL_FIELDS = ("ODQ_CHANGEMODE", "ODQ_ENTITYCNTR", "ODQ_TSN", "ODQ_UNITNO", "ODQ_RECORDNO")
CHANGE_MODE_FIELD = "ODQ_CHANGEMODE"


def subscriber_process_for(connection_id: str, context: str, odp_name: str) -> str:
    """A stable, SAP-safe subscriber-process name for one stream.

    Deterministic so that a re-created connection resumes its existing ODQ
    subscription instead of stranding it and starting a second one.
    """
    digest = hashlib.sha1(f"{connection_id}|{context}|{odp_name}".encode()).hexdigest()[:10].upper()
    readable = re.sub(r"[^A-Za-z0-9]+", "_", odp_name).strip("_").upper()
    budget = _MAX_SUBSCRIBER_PROCESS - len(_PREFIX) - len(digest) - 1
    return f"{_PREFIX}{readable[:budget]}_{digest}"


def _lit(value: str) -> str:
    return quote_identifier(str(value))


class OdpRfcDriver(ProtocolDriver):
    mode = "odp_rfc"
    required_extensions = ("erpl_rfc", "erpl_odp")

    # ---- discovery ------------------------------------------------------------

    @retry_transient()
    def check(self, session: ErplSession) -> str:
        cursor = session.cursor()
        try:
            cursor.execute("PRAGMA sap_rfc_ping").fetchone()
            contexts = cursor.execute("SELECT technical_name FROM sap_odp_show_contexts()").fetchall()
        except Exception as exc:
            raise traced("SAP ODP connection test failed", exc) from exc
        names = ", ".join(row[0] for row in contexts) or "none"
        return f"Connected to SAP ODP. Available contexts: {names}."

    def discover(self, session: ErplSession) -> list[SapObject]:
        cursor = session.cursor()
        objects: list[SapObject] = []
        for context, odp_name, override in self._selected(session):
            try:
                row = cursor.execute(
                    "SELECT supports_full, supports_delta, fields "
                    f"FROM sap_odp_describe({_lit(context)}, {_lit(odp_name)})"
                ).fetchone()
            except Exception as exc:
                logger.warning("Skipping ODP %s/%s: %s", context, odp_name, exc)
                continue
            if not row:
                continue
            supports_delta, fields = bool(row[1]), list(row[2] or [])
            if not fields:
                logger.warning("Skipping ODP %s/%s: no fields reported.", context, odp_name)
                continue
            business = [f for f in fields if f.get("technical_name") not in ODP_CONTROL_FIELDS]
            subscriber = override.get("subscriber_process") or subscriber_process_for(
                self._connection_id(), context, odp_name
            )
            objects.append(
                SapObject(
                    name=f"{context}/{odp_name}",
                    json_schema=json_schema_for_fields(fields),
                    primary_key=primary_key_for_fields(business),
                    supports_incremental=supports_delta,
                    change_mode_field=CHANGE_MODE_FIELD if supports_delta else None,
                    meta={
                        "context": context,
                        "odp_name": odp_name,
                        "subscriber_process": subscriber,
                        "supports_delta": supports_delta,
                    },
                )
            )
        return objects

    def _connection_id(self) -> str:
        return str(self.config.get("connection_id") or self.options.get("connection_id") or "airbyte")

    def _selected(self, session: ErplSession) -> list[tuple[str, str, Mapping[str, Any]]]:
        overrides = self._object_overrides()
        context = (self.options.get("context") or "").strip()
        pattern = (self.options.get("name_pattern") or "").strip()
        selected: list[tuple[str, str, Mapping[str, Any]]] = []
        seen: set[tuple[str, str]] = set()

        if pattern:
            if not context:
                raise config_error(
                    "An ODP context is required alongside 'name_pattern'. Pick one of the contexts "
                    "reported by the connection check, for example 'ABAP_CDS', 'BW' or 'SAPI'."
                )
            try:
                rows = (
                    session.cursor()
                    .execute(
                        f"SELECT technical_name FROM sap_odp_show({_lit(context)}, search := ?) ORDER BY 1",
                        [pattern],
                    )
                    .fetchall()
                )
            except Exception as exc:
                raise traced(f"Could not list ODP providers in context {context!r}", exc) from exc
            for row in rows:
                key = (context, row[0])
                if key not in seen:
                    seen.add(key)
                    selected.append((context, row[0], overrides.get(row[0], {})))

        for name, override in overrides.items():
            ctx = str(override.get("context") or context or "").strip()
            if not ctx:
                raise config_error(
                    f"ODP object {name!r} has no context. Set 'context' on the object or at the protocol level."
                )
            if (ctx, name) not in seen:
                seen.add((ctx, name))
                selected.append((ctx, name, override))

        if not selected:
            raise config_error(
                "No ODP providers selected. Set 'context' plus 'name_pattern', and/or list "
                "providers explicitly under 'objects'."
            )
        return selected

    # ---- reading --------------------------------------------------------------

    def read_plans(
        self, session: ErplSession, obj: SapObject, *, incremental: bool, state: Mapping[str, Any]
    ) -> list[ReadPlan]:
        context = str(obj.meta["context"])
        odp_name = str(obj.meta["odp_name"])
        override = self._object_overrides().get(odp_name, {})

        if incremental:
            subscriber = str(state.get("subscriber_process") or obj.meta["subscriber_process"])
            args = [_lit(context), _lit(odp_name), _lit(subscriber)]
            # Deliberately no THREADS: erpl caps delta fetch to one worker because
            # a parallel multi-package delta can silently under-count.
            self._append_projection(args, override)
            sql = f"SELECT * FROM sap_odp_read_delta({', '.join(args)})"
            return [ReadPlan(sql=sql, slice_={"odp": odp_name, "mode": "delta"})]

        args = [_lit(context), _lit(odp_name)]
        threads = override.get("threads", self.options.get("threads"))
        if threads not in (None, ""):
            args.append(f"THREADS := {int(threads)}")
        self._append_projection(args, override)
        sql = f"SELECT * FROM sap_odp_read_full({', '.join(args)})"
        return [ReadPlan(sql=sql, slice_={"odp": odp_name, "mode": "full"})]

    @staticmethod
    def _append_projection(args: list[str], override: Mapping[str, Any]) -> None:
        columns = override.get("columns")
        if columns:
            args.append("COLUMNS := [" + ", ".join(_lit(c) for c in columns) + "]")
        filters = override.get("filters")
        if filters:
            rendered = []
            for f in filters:
                pairs = ", ".join(
                    f"{_lit(str(v))} AS {key}"
                    for key, v in (
                        ("FIELDNAME", f.get("fieldname")),
                        ("SIGN", f.get("sign", "I")),
                        ("OP", f.get("op", "EQ")),
                        ("LOW", f.get("low", "")),
                        ("HIGH", f.get("high", "")),
                    )
                )
                rendered.append(f"{{{pairs}}}")
            args.append("FILTERS := [" + ", ".join(rendered) + "]")

    # ---- state ----------------------------------------------------------------

    def next_state(self, session: ErplSession, obj: SapObject, previous: Mapping[str, Any]) -> Mapping[str, Any]:
        state = dict(previous)
        state["subscriber_process"] = str(previous.get("subscriber_process") or obj.meta["subscriber_process"])
        state["context"] = obj.meta["context"]
        state["odp_name"] = obj.meta["odp_name"]
        state["initialized"] = True
        return state

    def on_success(self, session: ErplSession, obj: SapObject, state: Mapping[str, Any]) -> None:
        """Release the server-side delta cursor; the subscription itself survives."""
        subscriber = state.get("subscriber_process")
        if not subscriber:
            return
        context, odp_name = str(obj.meta["context"]), str(obj.meta["odp_name"])
        try:
            result = (
                session.cursor()
                .execute(f"PRAGMA sap_odp_close_delta_cursor({_lit(context)}, {_lit(subscriber)}, {_lit(odp_name)})")
                .fetchone()
            )
        except Exception as exc:
            logger.warning("Could not close the ODP delta cursor for %s: %s", obj.name, exc)
            return
        status = result[0] if result else "UNKNOWN"
        if status == "CLOSED":
            logger.info("Closed the ODP delta cursor for %s.", obj.name)
        elif status == "NOT_FOUND":
            logger.debug("No open ODP delta cursor for %s.", obj.name)
        else:
            # REFUSED / STILL_OPEN leave a cursor reserved on the SAP side.
            logger.warning(
                "The ODP delta cursor for %s could not be closed (%s). It stays reserved on SAP; "
                "the next sync recovers it, or clear it with PRAGMA sap_odp_drop.",
                obj.name,
                status,
            )

    def concurrency_group(self, obj: SapObject) -> str:
        # Two delta reads sharing a subscriber process would race the server-side pointer.
        return str(obj.meta.get("subscriber_process") or "")
