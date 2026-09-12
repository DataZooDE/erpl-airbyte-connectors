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


class TestSessionIdUniqueness:
    """Each slice runs its own BICS session; colliding ids interleave results."""

    def test_slices_of_a_long_stream_name_keep_distinct_sessions(self):
        # BW technical names routinely run to 30 characters. Truncating after
        # appending the member would give every slice the same session id, and
        # the Concurrent CDK reads slices in parallel.
        long_name = "ZQUERY_WITH_A_VERY_LONG_TECHNICAL_NAME_0001"
        d = driver(
            objects=[
                {
                    "name": long_name,
                    "cube": "C",
                    "slice_by": {"characteristic": "0CALMONTH", "members": ["202601", "202602", "202603"]},
                }
            ]
        )
        from source_sap.protocols.base import SapObject

        obj = SapObject(name=long_name, json_schema={}, meta={"cube": "C", "query": None, "session_id": "x"})
        ids = {p.slice_["session_id"] for p in d.read_plans(None, obj, incremental=False, state={})}
        assert len(ids) == 3

    def test_session_ids_stay_within_the_sap_field(self):
        from source_sap.protocols.bics import session_id_for

        got = session_id_for("Z" * 80, "202601")
        assert len(got) <= 40

    def test_session_ids_are_stable(self):
        from source_sap.protocols.bics import session_id_for

        assert session_id_for("Q", "a") == session_id_for("Q", "a")

    def test_different_streams_do_not_collide(self):
        from source_sap.protocols.bics import session_id_for

        assert session_id_for("A" * 60, "") != session_id_for("B" * 60, "")


class TestGrandTotalWithoutARowAxis:
    def test_every_string_column_is_checked_when_no_row_axis_is_known(self):
        # Otherwise a query with no `rows` configured emits the total as a fact.
        assert is_grand_total_row({"0CALDAY": "Overall Result", "AMOUNT": 1.0})

    def test_ordinary_rows_still_pass(self):
        assert not is_grand_total_row({"0CALDAY": "20260101", "AMOUNT": 1.0})

    def test_non_string_cells_are_ignored(self):
        assert not is_grand_total_row({"0CALDAY": None, "AMOUNT": 1.0})


class TestBicsIncremental:
    """BICS has no change tracking, but a BEx variable can carry a watermark.

    A query restricted by a period variable can be re-run with the variable set
    from the highest period seen last time, which is as close to incremental as
    BW gets without an ODP extractor behind it.
    """

    def _obj(self, **meta):
        return SapObject(name="Q", json_schema={}, meta={"cube": "C", "query": "Q", "session_id": "s", **meta})

    def _driver(self, **obj):
        return driver(objects=[{"name": "Q", "cube": "C", **obj}])

    def test_a_cursor_variable_makes_the_stream_incremental(self):
        d = self._driver(cursor_variable="ZVAR_MONTH", cursor_field="0CALMONTH")
        assert d.supports_incremental({"cursor_variable": "ZVAR_MONTH", "cursor_field": "0CALMONTH"})

    def test_a_cursor_field_alone_is_not_enough(self):
        # Without a variable there is nothing to restrict the query with, so the
        # "incremental" run would re-read everything and dedupe client-side.
        d = self._driver(cursor_field="0CALMONTH")
        assert not d.supports_incremental({"cursor_field": "0CALMONTH"})

    def test_the_state_value_fills_the_variable(self):
        d = self._driver(cursor_variable="ZVAR_MONTH", cursor_field="0CALMONTH")
        stmts = d.session_statements(
            self._obj(cursor_variable="ZVAR_MONTH", cursor_field="0CALMONTH"),
            state={"0CALMONTH": "202603"},
        )
        assert "'ZVAR_MONTH' AS NAME" in stmts[0]
        assert "'202603' AS LOW" in stmts[0]

    def test_the_watermark_uses_a_greater_or_equal_style_selection(self):
        d = self._driver(cursor_variable="ZVAR_MONTH", cursor_field="0CALMONTH")
        stmts = d.session_statements(
            self._obj(cursor_variable="ZVAR_MONTH", cursor_field="0CALMONTH"),
            state={"0CALMONTH": "202603"},
        )
        # BW's selection options: GE is the watermark shape.
        assert "'GE' AS OP" in stmts[0]

    def test_without_state_the_configured_start_is_used(self):
        d = self._driver(cursor_variable="ZVAR_MONTH", cursor_field="0CALMONTH", cursor_start="202601")
        stmts = d.session_statements(self._obj(cursor_variable="ZVAR_MONTH", cursor_field="0CALMONTH"), state={})
        assert "'202601' AS LOW" in stmts[0]

    def test_without_state_or_a_start_the_variable_is_left_unset(self):
        d = self._driver(cursor_variable="ZVAR_MONTH", cursor_field="0CALMONTH")
        stmts = d.session_statements(self._obj(cursor_variable="ZVAR_MONTH", cursor_field="0CALMONTH"), state={})
        assert "ZVAR_MONTH" not in stmts[0]

    def test_an_explicit_variable_binding_is_not_overwritten(self):
        d = self._driver(
            cursor_variable="ZVAR_MONTH", cursor_field="0CALMONTH", variables=[{"name": "ZVAR_REGION", "low": "EU"}]
        )
        stmts = d.session_statements(
            self._obj(cursor_variable="ZVAR_MONTH", cursor_field="0CALMONTH"),
            state={"0CALMONTH": "202603"},
        )
        assert "'ZVAR_REGION' AS NAME" in stmts[0] and "'ZVAR_MONTH' AS NAME" in stmts[0]

    def test_the_watermark_value_is_escaped(self):
        d = self._driver(cursor_variable="ZVAR_MONTH", cursor_field="0CALMONTH")
        stmts = d.session_statements(
            self._obj(cursor_variable="ZVAR_MONTH", cursor_field="0CALMONTH"),
            state={"0CALMONTH": "a' OR '1'='1"},
        )
        assert stmts[0].count("'") % 2 == 0
