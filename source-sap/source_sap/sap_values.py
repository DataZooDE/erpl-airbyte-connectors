"""Parsing SAP's scalar wire formats.

SAP writes a date as ``20260102`` and a time as ``103000``; a connector config is
JSON, so values arrive as strings in either the SAP form or the ISO one. These
accept both and emit ISO, which is what DuckDB and JSON Schema want.

Separate from any one protocol: these are facts about SAP, not about calling
function modules.
"""

from __future__ import annotations

import datetime

_DATE_FORMATS = ("%Y%m%d", "%Y-%m-%d")
#: Keyed by length, because SAP's digit-only times are fixed-width and
#: `strptime("1030", "%H%M%S")` otherwise parses greedily as 10:03:00.
_TIME_BY_LENGTH = {6: "%H%M%S", 4: "%H%M"}
_TIME_FORMATS = ("%H:%M:%S", "%H:%M")
_TIMESTAMP_FORMATS = ("%Y%m%d%H%M%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S")


def _parse(value: str, formats: tuple[str, ...], what: str, shape: str) -> datetime.datetime:
    text = str(value).strip()
    for fmt in formats:
        try:
            return datetime.datetime.strptime(text, fmt)
        except ValueError:
            continue
    raise ValueError(f"{value!r} is not a {what} (expected {shape})")


def sap_date(value: str) -> str:
    """SAP DATS or ISO in, ISO date out."""
    return _parse(value, _DATE_FORMATS, "date", "YYYYMMDD or YYYY-MM-DD").date().isoformat()


def sap_time(value: str) -> str:
    """SAP TIMS or ISO in, ISO time out."""
    text = str(value).strip()
    formats = _TIME_FORMATS
    if text.isdigit():
        fmt = _TIME_BY_LENGTH.get(len(text))
        if fmt is None:
            raise ValueError(f"{value!r} is not a time (expected HHMMSS or HH:MM:SS)")
        formats = (fmt,)
    return _parse(text, formats, "time", "HHMMSS or HH:MM:SS").time().isoformat()


def sap_timestamp(value: str) -> str:
    """SAP UTC long form or ISO in, ISO timestamp out."""
    return _parse(value, _TIMESTAMP_FORMATS, "timestamp", "YYYYMMDDHHMMSS or an ISO timestamp").isoformat(sep=" ")
