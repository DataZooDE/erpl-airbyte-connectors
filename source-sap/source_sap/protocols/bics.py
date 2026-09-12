"""SAP BW cubes and BEx queries over BICS (`erpl_bics`).

BICS is a *stateful* protocol: open a session, configure axes and filters, then
fetch.  And it cannot paginate -- BW materialises the entire result set or none
of it -- so the only way to bound memory on a large cube is to slice the query
yourself on a characteristic.  Each slice is a separate BICS session, which is
what makes slices independently readable.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping, Sequence
from typing import Any

from source_sap.duck import schema_from_description
from source_sap.errors import config_error, traced
from source_sap.protocols.base import ProtocolDriver, ReadPlan, SapObject, quote_identifier
from source_sap.retry import retry_transient
from source_sap.session import ErplSession

logger = logging.getLogger("airbyte")

# BW appends a grand-total row to every result; it is not a fact and must not
# be emitted as one.
_TOTAL_LABELS = {
    "SUMME",
    "GESAMTERGEBNIS",
    "OVERALL RESULT",
    "RESULT",
    "ERGEBNIS",
    "TOTAL",
    "GESAMT",
}


def _lit(value: Any) -> str:
    return quote_identifier(str(value))


def is_grand_total_row(record: Mapping[str, Any], row_axis_fields: Sequence[str]) -> bool:
    """True when a BICS row is BW's grand total rather than a data row."""
    for field in row_axis_fields:
        value = record.get(field)
        if isinstance(value, str) and value.strip().upper() in _TOTAL_LABELS:
            return True
    return False


def _session_id(stream_name: str, suffix: str = "") -> str:
    base = re.sub(r"[^A-Za-z0-9]+", "_", stream_name).strip("_").lower()
    return f"abyte_{base}{('_' + suffix) if suffix else ''}"[:40]


class BicsDriver(ProtocolDriver):
    mode = "bics"
    required_extensions = ("erpl_rfc", "erpl_bics")

    # ---- discovery ------------------------------------------------------------

    @retry_transient()
    def check(self, session: ErplSession) -> str:
        cursor = session.cursor()
        try:
            cursor.execute("PRAGMA sap_rfc_ping").fetchone()
            rows = cursor.execute("SELECT count(*) FROM sap_bics_show_cubes()").fetchone()
        except Exception as exc:
            raise traced("SAP BICS connection test failed", exc) from exc
        return f"Connected to SAP BW via BICS ({rows[0] if rows else 0} InfoProviders visible)."

    def discover(self, session: ErplSession) -> list[SapObject]:
        cursor = session.cursor()
        objects: list[SapObject] = []
        for name, override in self._selected(session).items():
            cube = str(override.get("cube") or override.get("query") or name)
            try:
                schema = self._describe(cursor, name, override, cube)
            except Exception as exc:
                logger.warning("Skipping BICS object %s: %s", name, exc)
                continue
            if schema is None:
                continue
            objects.append(
                SapObject(
                    name=name,
                    json_schema=schema,
                    supports_incremental=False,  # BICS exposes no change tracking
                    meta={
                        "cube": cube,
                        "query": override.get("query"),
                        "session_id": _session_id(name),
                        "row_axis": list(override.get("rows") or []),
                    },
                )
            )
        return objects

    def _describe(self, cursor: Any, name: str, override: Mapping[str, Any], cube: str) -> dict[str, Any] | None:
        """Open a DESCRIBE session and read the result columns."""
        self._assert_mandatory_variables_bound(cursor, override, cube)
        obj = SapObject(
            name=name,
            json_schema={},
            meta={
                "cube": cube,
                "query": override.get("query"),
                "session_id": _session_id(name, "d"),
            },
        )
        statements = self.session_statements(obj)
        for statement in statements:
            cursor.execute(statement)
        schema = schema_from_description(cursor.description)
        return schema if schema["properties"] else None

    def _assert_mandatory_variables_bound(self, cursor: Any, override: Mapping[str, Any], cube: str) -> None:
        """BW refuses to produce a result while a mandatory BEx variable is unbound."""
        query = override.get("query")
        if not query:
            return
        bound = {str(v.get("name", "")).upper() for v in (override.get("variables") or [])}
        try:
            rows = cursor.execute(
                f"SELECT name, mandatory, input_enabled FROM sap_bics_variables({_lit(cube)}, query := {_lit(query)})"
            ).fetchall()
        except Exception:
            return  # variable introspection is a nicety, not a gate
        missing = [r[0] for r in rows if r[1] and r[2] and str(r[0]).upper() not in bound]
        if missing:
            raise config_error(
                f"BEx query {query!r} has mandatory variables that are not bound: "
                f"{', '.join(missing)}. Add them to the object's 'variables' list."
            )

    def _selected(self, session: ErplSession) -> dict[str, Mapping[str, Any]]:
        overrides = dict(self._object_overrides())
        pattern = (self.options.get("query_pattern") or "").strip()
        if pattern:
            obj_type = (self.options.get("object_type") or "QUERY").upper()
            try:
                rows = (
                    session.cursor()
                    .execute(
                        "SELECT technical_name, cube_name FROM sap_bics_show(obj_type := ?, search := ?) "
                        "WHERE NOT is_folder ORDER BY 1",
                        [obj_type, pattern],
                    )
                    .fetchall()
                )
            except Exception as exc:
                raise traced(f"Could not list BW objects matching {pattern!r}", exc) from exc
            for technical_name, cube_name in rows:
                overrides.setdefault(
                    technical_name,
                    {"cube": cube_name or technical_name, "query": technical_name if obj_type == "QUERY" else None},
                )
        if not overrides:
            raise config_error(
                "No BW objects selected. Set 'query_pattern' (for example '*SALES*') and/or list "
                "cubes and BEx queries explicitly under 'objects'."
            )
        return overrides

    # ---- the stateful workflow ------------------------------------------------

    def session_statements(self, obj: SapObject, extra_filter: tuple[str, Sequence[str]] | None = None) -> list[str]:
        """The full begin -> configure -> result sequence for one BICS session."""
        override = self._object_overrides().get(obj.name, {})
        session_id = str(obj.meta["session_id"])
        cube = str(obj.meta["cube"])

        begin_args = [_lit(cube), f"id := {_lit(session_id)}", "return := 'RESULT'"]
        variables = override.get("variables") or []
        if variables:
            rendered = []
            for var in variables:
                low = str(var.get("low", ""))
                high = str(var.get("high", ""))
                op = var.get("op") or ("BT" if high else "EQ")
                rendered.append(
                    "{"
                    + ", ".join(
                        [
                            f"{_lit(var.get('name', ''))} AS NAME",
                            f"{_lit(var.get('sign', 'I'))} AS SIGN",
                            f"{_lit(op)} AS OP",
                            f"{_lit(low)} AS LOW",
                            f"{_lit(high)} AS HIGH",
                        ]
                    )
                    + "}"
                )
            begin_args.append("variables := [" + ", ".join(rendered) + "]")
        variant = override.get("variant")
        if variant:
            begin_args.append(f"variant := {_lit(variant)}")

        statements = [f"SELECT * FROM sap_bics_begin({', '.join(begin_args)})"]

        rows = override.get("rows") or []
        if rows:
            args = ", ".join(_lit(r) for r in rows)
            statements.append(f"SELECT * FROM sap_bics_rows({_lit(session_id)}, {args}, op := 'SET')")
        columns = override.get("columns") or []
        if columns:
            args = ", ".join(_lit(c) for c in columns)
            statements.append(f"SELECT * FROM sap_bics_columns({_lit(session_id)}, {args}, op := 'SET')")

        for member_filter in override.get("filters") or []:
            characteristic = member_filter.get("characteristic")
            members = member_filter.get("members") or []
            if not characteristic:
                continue
            rendered = "".join(f", {_lit(m)}" for m in members)
            statements.append(
                f"SELECT * FROM sap_bics_filter({_lit(session_id)}, {_lit(characteristic)}{rendered}, op := 'SET')"
            )

        if extra_filter is not None:
            characteristic, members = extra_filter
            rendered = "".join(f", {_lit(m)}" for m in members)
            statements.append(
                f"SELECT * FROM sap_bics_filter({_lit(session_id)}, {_lit(characteristic)}{rendered}, op := 'SET')"
            )

        for prop in override.get("properties") or []:
            statements.append(
                "SELECT * FROM sap_bics_set_char_prop("
                f"{_lit(session_id)}, {_lit(prop.get('characteristic', ''))}, "
                f"{_lit(prop.get('property', ''))}, {_lit(prop.get('value', ''))})"
            )

        statements.append(f"SELECT * FROM sap_bics_result({_lit(session_id)})")
        return statements

    # ---- reading --------------------------------------------------------------

    def read_plans(
        self, session: ErplSession, obj: SapObject, *, incremental: bool, state: Mapping[str, Any]
    ) -> list[ReadPlan]:
        override = self._object_overrides().get(obj.name, {})
        slice_by = override.get("slice_by") or {}
        characteristic = slice_by.get("characteristic")
        members = slice_by.get("members") or []

        if not (characteristic and members):
            statements = self.session_statements(obj)
            return [
                ReadPlan(
                    sql=statements[-1],
                    slice_={
                        "session_id": obj.meta["session_id"],
                        "setup": statements[:-1],
                    },
                )
            ]

        plans: list[ReadPlan] = []
        for member in members:
            sliced = SapObject(
                name=obj.name,
                json_schema=obj.json_schema,
                primary_key=obj.primary_key,
                supports_incremental=obj.supports_incremental,
                change_mode_field=obj.change_mode_field,
                meta={**obj.meta, "session_id": _session_id(obj.name, str(member))},
            )
            statements = self.session_statements(sliced, extra_filter=(characteristic, [member]))
            plans.append(
                ReadPlan(
                    sql=statements[-1],
                    slice_={
                        "session_id": sliced.meta["session_id"],
                        "characteristic": characteristic,
                        "member": member,
                        "setup": statements[:-1],
                    },
                )
            )
        return plans
