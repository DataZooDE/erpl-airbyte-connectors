"""BICS driver: session workflow, variables, and the grand-total row."""

import pytest

from source_sap.protocols.base import SapObject
from source_sap.protocols.bics import BicsDriver, is_grand_total_row


def driver(**protocol):
    return BicsDriver(
        {
            "ashost": "h",
            "sysnr": "00",
            "client": "001",
            "user": "u",
            "password": "p",
            "protocol": {"mode": "bics", **protocol},
        }
    )


class TestGrandTotalRow:
    @pytest.mark.parametrize("value", ["SUMME", "Overall Result", "Gesamtergebnis", "Result"])
    def test_known_total_labels_are_detected(self, value):
        assert is_grand_total_row({"0CALDAY": value, "AMOUNT": 1.0}, ["0CALDAY"])

    def test_ordinary_rows_are_kept(self):
        assert not is_grand_total_row({"0CALDAY": "20260101", "AMOUNT": 1.0}, ["0CALDAY"])

    def test_only_row_axis_columns_are_examined(self):
        # A key figure that happens to be the string "Result" must not drop the row.
        assert not is_grand_total_row({"0CALDAY": "20260101", "TEXT": "Result"}, ["0CALDAY"])


class TestSessionWorkflow:
    def _obj(self, **meta):
        return SapObject(
            name="Q1", json_schema={}, meta={"cube": "MY_CUBE", "query": "Q1", "session_id": "abyte_Q1", **meta}
        )

    def test_begin_binds_variables(self):
        d = driver(objects=[{"name": "Q1", "cube": "MY_CUBE", "variables": [{"name": "ZVAR_YEAR", "low": "2026"}]}])
        stmts = d.session_statements(self._obj())
        begin = stmts[0]
        assert "sap_bics_begin('MY_CUBE'" in begin
        assert "'ZVAR_YEAR' AS NAME" in begin
        assert "'2026' AS LOW" in begin

    def test_variable_defaults_to_include_equals(self):
        d = driver(objects=[{"name": "Q1", "cube": "MY_CUBE", "variables": [{"name": "V", "low": "1"}]}])
        begin = d.session_statements(self._obj())[0]
        assert "'I' AS SIGN" in begin and "'EQ' AS OP" in begin

    def test_interval_variable_uses_between(self):
        d = driver(objects=[{"name": "Q1", "cube": "MY_CUBE", "variables": [{"name": "V", "low": "1", "high": "9"}]}])
        begin = d.session_statements(self._obj())[0]
        assert "'BT' AS OP" in begin and "'9' AS HIGH" in begin

    def test_rows_and_columns_become_axis_calls(self):
        d = driver(objects=[{"name": "Q1", "cube": "MY_CUBE", "rows": ["0CALDAY"], "columns": ["0AMOUNT"]}])
        stmts = " | ".join(d.session_statements(self._obj()))
        assert "sap_bics_rows('abyte_Q1', '0CALDAY'" in stmts
        assert "sap_bics_columns('abyte_Q1', '0AMOUNT'" in stmts

    def test_member_filters_become_filter_calls(self):
        d = driver(
            objects=[
                {"name": "Q1", "cube": "MY_CUBE", "filters": [{"characteristic": "0CNTRY", "members": ["DE", "FR"]}]}
            ]
        )
        stmts = " | ".join(d.session_statements(self._obj()))
        assert "sap_bics_filter('abyte_Q1', '0CNTRY', 'DE', 'FR'" in stmts

    def test_result_is_the_last_statement(self):
        d = driver(objects=[{"name": "Q1", "cube": "MY_CUBE"}])
        assert d.session_statements(self._obj())[-1].startswith("SELECT * FROM sap_bics_result(")

    def test_variable_values_are_escaped(self):
        d = driver(objects=[{"name": "Q1", "cube": "MY_CUBE", "variables": [{"name": "V", "low": "a' OR '1'='1"}]}])
        begin = d.session_statements(self._obj())[0]
        assert "'a'' OR ''1''=''1' AS LOW" in begin


class TestSlicing:
    def _obj(self):
        return SapObject(name="Q1", json_schema={}, meta={"cube": "MY_CUBE", "query": "Q1", "session_id": "abyte_Q1"})

    def test_slices_produce_one_plan_each(self):
        # BICS cannot paginate: BW builds the whole result or none of it. Slicing
        # on a characteristic is the only way to bound memory.
        d = driver(
            objects=[
                {
                    "name": "Q1",
                    "cube": "MY_CUBE",
                    "slice_by": {"characteristic": "0CALMONTH", "members": ["202601", "202602", "202603"]},
                }
            ]
        )
        plans = d.read_plans(None, self._obj(), incremental=False, state={})
        assert len(plans) == 3
        assert plans[0].slice_["member"] == "202601"

    def test_without_slicing_there_is_one_plan(self):
        d = driver(objects=[{"name": "Q1", "cube": "MY_CUBE"}])
        assert len(d.read_plans(None, self._obj(), incremental=False, state={})) == 1

    def test_each_slice_gets_its_own_session_id(self):
        d = driver(
            objects=[
                {"name": "Q1", "cube": "MY_CUBE", "slice_by": {"characteristic": "0CALMONTH", "members": ["1", "2"]}}
            ]
        )
        plans = d.read_plans(None, self._obj(), incremental=False, state={})
        ids = {p.slice_["session_id"] for p in plans}
        assert len(ids) == 2
