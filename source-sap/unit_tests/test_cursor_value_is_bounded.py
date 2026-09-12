"""A cursor value comes back from state, and state is not always ours.

Quoting is handled -- `_sql_literal` doubles quotes at both levels -- but length
is not: SAP's RFC_READ_TABLE takes its WHERE fragment as 72-character lines, and
a value of a few kilobytes produces a dump or a truncation rather than our own
error message. A checkpointed DATS value is 8 characters; anything approaching
this bound is a corrupted or forged state blob.
"""

import pytest

from source_sap.protocols.base import SapObject
from source_sap.protocols.rfc import MAX_CURSOR_VALUE, RfcDriver

CONN = {"ashost": "h", "sysnr": "00", "client": "001", "user": "u", "password": "p"}


def _plans(value, sap_type="DATS"):
    driver = RfcDriver({**CONN, "protocol": {"mode": "rfc", "objects": [{"name": "T", "cursor_field": "ERDAT"}]}})
    target = SapObject(
        name="T",
        json_schema={},
        supports_incremental=True,
        meta={"table": "T", "cursor_field": "ERDAT", "cursor_sap_type": sap_type},
    )
    return driver.read_plans(None, target, incremental=True, state={"ERDAT": value})


class TestAnOverLongCursorValueIsRejected:
    def test_a_normal_value_is_used(self):
        assert "20260905" in _plans("2026-09-05")[0].sql

    def test_a_value_past_the_bound_raises_our_error(self):
        with pytest.raises(ValueError) as caught:
            _plans("A" * (MAX_CURSOR_VALUE + 1), sap_type="CHAR")
        assert "ERDAT" in str(caught.value)

    def test_the_bound_itself_is_allowed(self):
        assert _plans("A" * MAX_CURSOR_VALUE, sap_type="CHAR")

    def test_a_long_date_is_still_truncated_not_rejected(self):
        # DATS keeps its first 8 characters, so length is not the question there.
        assert "20260905" in _plans("2026-09-05T00:00:00.000000+00:00")[0].sql
