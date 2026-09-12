"""SAP tables and CDS views over RFC (`erpl_rfc`)."""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping, Sequence
from typing import Any

from source_sap.errors import config_error, traced
from source_sap.protocols.base import (
    ProtocolDriver,
    ReadPlan,
    SapObject,
    clamp,
    sql_string_literal,
)
from source_sap.retry import retry_transient
from source_sap.session import ErplSession
from source_sap.types import (
    json_schema_for_fields,
    primary_key_for_fields,
    resume_key_field,
)

logger = logging.getLogger("airbyte")


def _sql_literal(value: str) -> str:
    return sql_string_literal(str(value))


CURSOR_FIELD_PATTERN = re.compile(r"^[A-Z0-9_/]{1,30}$")

#: State key holding where an interrupted full refresh should continue from.
RESUME_FIELD = "__resume_key"


def sap_cursor_literal(value: Any, sap_type: str | None) -> str:
    """Render a state value as the ABAP literal SAP expects.

    State travels as JSON, so a DATS column checkpoints as "2026-09-05" -- and
    SAP rejects that with *"'2026-09-05' is not a valid value for D(8,0)"*.
    Dates, times and timestamps have to go back to their compact DDIC form.
    """
    text = str(value)
    kind = (sap_type or "").strip().upper()
    if kind == "DATS":
        return text.replace("-", "")[:8]
    if kind == "TIMS":
        return text.replace(":", "")[:6]
    if kind in ("UTCLONG", "UTCL", "UTCS", "UTCM", "TIMESTAMP"):
        return "".join(c for c in text if c.isdigit())[:14]
    return text


class RfcDriver(ProtocolDriver):
    mode = "rfc"
    required_extensions = ("erpl_rfc",)

    def partitions_for(self, obj: SapObject) -> int:
        """Effective partition count for a stream, object setting winning."""
        override = self._object_overrides().get(obj.name, {})
        return clamp(override.get("partitions", self.options.get("partitions")), 0, 64) or 0

    def is_resumable(self, obj: SapObject, partitions: int | None = None) -> bool:
        """Whether a full refresh of this stream can pick up where it left off.

        Only an *unpartitioned* scan is ordered (ERPL asks SAP for a sorted read);
        partitioned workers return their windows in whatever order they finish,
        so a resume key would skip whatever a slower worker had not yet emitted.
        """
        if partitions is None:
            partitions = self.partitions_for(obj)
        return bool(obj.meta.get("resume_key")) and not partitions

    def resume_key_field(self, obj: SapObject) -> str | None:
        value = obj.meta.get("resume_key")
        return str(value) if value else None

    def validate_cursor_field(self, cursor_field: str, known_fields: Sequence[str]) -> str:
        """The cursor field is interpolated bare into the ABAP WHERE fragment.

        Every other fragment goes through `sql_string_literal`; an identifier
        cannot, so it is checked against the shape SAP allows *and* against the
        fields the table actually has.
        """
        candidate = str(cursor_field).strip().upper()
        if not CURSOR_FIELD_PATTERN.match(candidate):
            raise config_error(
                f"cursor_field {cursor_field!r} is not a valid SAP field name. It must be "
                "1-30 characters of A-Z, 0-9, underscore and slash."
            )
        if candidate not in {str(f).upper() for f in known_fields}:
            raise config_error(
                f"cursor_field {cursor_field!r} is not a field of this table. Available "
                f"fields: {', '.join(sorted(str(f) for f in known_fields))}."
            )
        return candidate

    # ---- discovery ------------------------------------------------------------

    @retry_transient()
    def check(self, session: ErplSession) -> str:
        try:
            result = session.cursor().execute("PRAGMA sap_rfc_ping").fetchone()
        except Exception as exc:
            raise traced("SAP RFC connection test failed", exc) from exc
        return f"Connected to SAP via RFC ({result[0] if result else 'ok'})."

    def discover(self, session: ErplSession) -> list[SapObject]:
        names = self._selected_table_names(session)
        cursor = session.cursor()
        objects: list[SapObject] = []
        for name in names:
            try:
                rows = cursor.execute(
                    f"SELECT pos, is_key, field, text, sap_type, length, decimals "
                    f"FROM sap_describe_fields({_sql_literal(name)}) ORDER BY pos"
                ).fetchall()
            except Exception as exc:
                logger.warning("Skipping %s: could not read its field list (%s)", name, exc)
                continue
            if not rows:
                logger.warning("Skipping %s: SAP reported no fields.", name)
                continue
            fields = [
                {
                    "technical_name": row[2],
                    "text": row[3],
                    "abap_type": row[4],
                    "length": int(row[5] or 0),
                    "decimals": int(row[6] or 0),
                    "key": row[1] in (True, "X", "x"),
                }
                for row in rows
            ]
            override = self._object_overrides().get(name, {})
            cursor_field = override.get("cursor_field")
            if cursor_field:
                cursor_field = self.validate_cursor_field(cursor_field, [f["technical_name"] for f in fields])
            cursor_sap_type = next((f["abap_type"] for f in fields if f["technical_name"] == cursor_field), None)
            primary_key = primary_key_for_fields(fields)
            objects.append(
                SapObject(
                    name=name,
                    json_schema=json_schema_for_fields(fields),
                    primary_key=primary_key,
                    supports_incremental=bool(cursor_field),
                    meta={
                        "table": name,
                        "cursor_field": cursor_field,
                        "cursor_sap_type": cursor_sap_type,
                        "resume_key": resume_key_field(fields),
                    },
                )
            )
        return objects

    def _selected_table_names(self, session: ErplSession) -> list[str]:
        overrides = self._object_overrides()
        pattern = (self.options.get("table_pattern") or "").strip()
        if not pattern and not overrides:
            raise config_error(
                "No SAP tables selected. Set 'table_pattern' (for example '/DMO/*' or 'SFLIGHT') "
                "and/or list tables explicitly under 'objects'."
            )
        names: list[str] = []
        if pattern:
            try:
                rows = (
                    session.cursor()
                    .execute("SELECT table_name FROM sap_show_tables(TABLENAME := ?) ORDER BY 1", [pattern])
                    .fetchall()
                )
            except Exception as exc:
                raise traced(f"Could not list SAP tables matching {pattern!r}", exc) from exc
            names.extend(row[0] for row in rows)
        for name in overrides:
            if name not in names:
                names.append(name)
        if not names:
            raise config_error(
                f"No SAP table matched the pattern {pattern!r}. Patterns use SAP wildcards "
                "('*' for any sequence of characters), and the name is case-sensitive."
            )
        return names

    # ---- reading --------------------------------------------------------------

    def read_plans(
        self, session: ErplSession, obj: SapObject, *, incremental: bool, state: Mapping[str, Any]
    ) -> list[ReadPlan]:
        table = str(obj.meta.get("table") or obj.name)
        override = self._object_overrides().get(obj.name, {})
        args: list[str] = [_sql_literal(table)]

        columns = override.get("columns") or self.options.get("columns")
        if columns:
            rendered = ", ".join(_sql_literal(c) for c in columns)
            args.append(f"COLUMNS := [{rendered}]")

        partitions = self.partitions_for(obj)

        predicates: list[str] = []
        user_filter = (override.get("filter") or "").strip()
        if user_filter:
            predicates.append(user_filter)

        resume_from = state.get(RESUME_FIELD)
        resume_field = obj.meta.get("resume_key")
        if (
            not incremental
            and resume_from not in (None, "")
            and resume_field
            and not partitions  # a partitioned scan is unordered; see is_resumable
        ):
            predicates.append(f"{resume_field} > {_sql_literal(str(resume_from))}")
        cursor_field = obj.meta.get("cursor_field") or override.get("cursor_field")
        if incremental and cursor_field:
            since = state.get(str(cursor_field))
            if since not in (None, ""):
                literal = sap_cursor_literal(since, obj.meta.get("cursor_sap_type"))
                # SAP's WHERE fragment uses ABAP literal quoting, and the whole
                # fragment is then a DuckDB string literal -- hence two levels.
                predicates.append(f"{cursor_field} >= {_sql_literal(literal)}")
        if predicates:
            combined = " AND ".join(f"( {p} )" for p in predicates) if len(predicates) > 1 else predicates[0]
            args.append(f"FILTER := {_sql_literal(combined)}")

        for cfg_key, sql_key, low, high in (
            ("partitions", "PARTITIONS", 0, 64),
            ("fetch_size", "FETCH_SIZE", 1, 4_000_000),
            ("threads", "THREADS", 0, 32),
            ("max_rows", "MAX_ROWS", 0, 2_147_483_647),
        ):
            value = clamp(override.get(cfg_key, self.options.get(cfg_key)), low, high)
            if value is not None:
                args.append(f"{sql_key} := {value}")

        sql = f"SELECT * FROM sap_read_table({', '.join(args)})"
        return [ReadPlan(sql=sql, slice_={"table": table})]

    def next_state(self, session: ErplSession, obj: SapObject, previous: Mapping[str, Any]) -> Mapping[str, Any]:
        # The cursor value is observed from the records themselves; see cursors.py.
        return dict(previous)

    def cursor_field(self, obj: SapObject) -> str | None:
        value = obj.meta.get("cursor_field")
        return str(value) if value else None
