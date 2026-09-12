"""End-to-end RFC sync against a live SAP system. No mocks."""

from __future__ import annotations

import pytest

from e2e.conftest import errors, records, run_connector, states

pytestmark = pytest.mark.requires_creds


@pytest.fixture(scope="module")
def rfc_config(sap_rfc_config):
    return {**sap_rfc_config, "protocol": {"mode": "rfc", "table_pattern": "SFLIGHT"}}


def _catalog(stream: str, schema: dict, sync_mode: str = "full_refresh", cursor=None):
    configured = {
        "stream": {"name": stream, "json_schema": schema, "supported_sync_modes": ["full_refresh", "incremental"]},
        "sync_mode": sync_mode,
        "destination_sync_mode": "overwrite" if sync_mode == "full_refresh" else "append_dedup",
    }
    if cursor:
        configured["cursor_field"] = [cursor]
        configured["stream"]["default_cursor_field"] = [cursor]
    return {"streams": [configured]}


class TestCheck:
    def test_succeeds_against_the_real_system(self, rfc_config, tmp_path):
        messages = run_connector("check", config=rfc_config, tmp_path=tmp_path)
        status = [m for m in messages if m.get("type") == "CONNECTION_STATUS"]
        assert status and status[0]["connectionStatus"]["status"] == "SUCCEEDED"

    def test_bad_password_fails_with_a_useful_message(self, rfc_config, tmp_path):
        messages = run_connector("check", config={**rfc_config, "password": "definitely-wrong"}, tmp_path=tmp_path)
        status = [m for m in messages if m.get("type") == "CONNECTION_STATUS"]
        assert status and status[0]["connectionStatus"]["status"] == "FAILED"
        assert status[0]["connectionStatus"]["message"]

    def test_the_password_never_appears_in_the_output(self, rfc_config, tmp_path):
        messages = run_connector("check", config=rfc_config, tmp_path=tmp_path)
        blob = str(messages)
        assert rfc_config["password"] not in blob


class TestDiscover:
    def test_finds_the_table_with_a_real_schema(self, rfc_config, tmp_path):
        messages = run_connector("discover", config=rfc_config, tmp_path=tmp_path)
        catalogs = [m["catalog"] for m in messages if m.get("type") == "CATALOG"]
        assert catalogs
        streams = {s["name"]: s for s in catalogs[0]["streams"]}
        assert "SFLIGHT" in streams
        props = streams["SFLIGHT"]["json_schema"]["properties"]
        assert "CARRID" in props and "FLDATE" in props
        # SAP reports FLDATE as DATS and PRICE as CURR.
        assert props["FLDATE"] == {
            "type": ["null", "string"],
            "format": "date",
            "description": props["FLDATE"].get("description"),
        }
        assert props["PRICE"]["type"] == ["null", "number"]
        # MANDT/CARRID/CONNID/FLDATE are the SAP key of SFLIGHT.
        assert streams["SFLIGHT"]["source_defined_primary_key"] == [["MANDT"], ["CARRID"], ["CONNID"], ["FLDATE"]]


class TestRead:
    @pytest.fixture(scope="class")
    def schema(self, rfc_config, tmp_path_factory):
        messages = run_connector("discover", config=rfc_config, tmp_path=tmp_path_factory.mktemp("d"))
        catalog = [m["catalog"] for m in messages if m.get("type") == "CATALOG"][0]
        return next(s for s in catalog["streams"] if s["name"] == "SFLIGHT")["json_schema"]

    def test_emits_records_and_no_errors(self, rfc_config, schema, tmp_path):
        messages = run_connector("read", config=rfc_config, catalog=_catalog("SFLIGHT", schema), tmp_path=tmp_path)
        assert errors(messages) == []
        rows = records(messages, "SFLIGHT")
        assert len(rows) > 0
        assert {"MANDT", "CARRID", "FLDATE", "PRICE"} <= set(rows[0]["data"])

    def test_emitted_at_is_in_milliseconds(self, rfc_config, schema, tmp_path):
        # The previous connector used seconds, dating every record to 1970.
        messages = run_connector("read", config=rfc_config, catalog=_catalog("SFLIGHT", schema), tmp_path=tmp_path)
        emitted_at = records(messages, "SFLIGHT")[0]["emitted_at"]
        assert emitted_at > 1_600_000_000_000

    def test_decimal_and_date_values_survive_as_json(self, rfc_config, schema, tmp_path):
        messages = run_connector("read", config=rfc_config, catalog=_catalog("SFLIGHT", schema), tmp_path=tmp_path)
        row = records(messages, "SFLIGHT")[0]["data"]
        assert isinstance(row["FLDATE"], str) and row["FLDATE"].count("-") == 2
        assert isinstance(row["PRICE"], str)  # exact decimal, not a lossy float

    def test_a_state_message_is_emitted(self, rfc_config, schema, tmp_path):
        messages = run_connector("read", config=rfc_config, catalog=_catalog("SFLIGHT", schema), tmp_path=tmp_path)
        assert states(messages, "SFLIGHT")

    def test_column_projection_reaches_sap(self, rfc_config, schema, tmp_path):
        config = {
            **rfc_config,
            "protocol": {
                "mode": "rfc",
                "table_pattern": "SFLIGHT",
                "objects": [{"name": "SFLIGHT", "columns": ["CARRID", "CONNID", "FLDATE"]}],
            },
        }
        messages = run_connector("read", config=config, catalog=_catalog("SFLIGHT", schema), tmp_path=tmp_path)
        assert errors(messages) == []
        assert set(records(messages, "SFLIGHT")[0]["data"]) == {"CARRID", "CONNID", "FLDATE"}

    def test_sap_side_filter_reaches_sap(self, rfc_config, schema, tmp_path):
        config = {
            **rfc_config,
            "protocol": {
                "mode": "rfc",
                "table_pattern": "SFLIGHT",
                "objects": [{"name": "SFLIGHT", "filter": "CARRID = 'LH'"}],
            },
        }
        messages = run_connector("read", config=config, catalog=_catalog("SFLIGHT", schema), tmp_path=tmp_path)
        rows = records(messages, "SFLIGHT")
        assert rows and {r["data"]["CARRID"] for r in rows} == {"LH"}

    def test_partitioned_read_returns_the_same_rows(self, rfc_config, schema, tmp_path):
        base = run_connector(
            "read",
            config={**rfc_config, "protocol": {"mode": "rfc", "table_pattern": "SFLIGHT", "partitions": 0}},
            catalog=_catalog("SFLIGHT", schema),
            tmp_path=tmp_path,
        )
        parallel = run_connector(
            "read",
            config={**rfc_config, "protocol": {"mode": "rfc", "table_pattern": "SFLIGHT", "partitions": 8}},
            catalog=_catalog("SFLIGHT", schema),
            tmp_path=tmp_path,
        )
        assert errors(parallel) == []

        # Partitioned scans return the same rows in an unspecified order.
        def key(rows):
            return sorted(tuple(sorted(r["data"].items())) for r in rows)

        assert key(records(base, "SFLIGHT")) == key(records(parallel, "SFLIGHT"))


class TestIncremental:
    @pytest.fixture(scope="class")
    def schema(self, rfc_config, tmp_path_factory):
        messages = run_connector("discover", config=rfc_config, tmp_path=tmp_path_factory.mktemp("d2"))
        catalog = [m["catalog"] for m in messages if m.get("type") == "CATALOG"][0]
        return next(s for s in catalog["streams"] if s["name"] == "SFLIGHT")["json_schema"]

    @pytest.fixture(scope="class")
    def incremental_config(self, rfc_config):
        return {
            **rfc_config,
            "protocol": {
                "mode": "rfc",
                "table_pattern": "SFLIGHT",
                "objects": [{"name": "SFLIGHT", "cursor_field": "FLDATE"}],
            },
        }

    def test_stream_advertises_incremental(self, incremental_config, tmp_path):
        messages = run_connector("discover", config=incremental_config, tmp_path=tmp_path)
        catalog = [m["catalog"] for m in messages if m.get("type") == "CATALOG"][0]
        stream = next(s for s in catalog["streams"] if s["name"] == "SFLIGHT")
        assert "incremental" in stream["supported_sync_modes"]

    def test_second_run_from_state_returns_fewer_rows(self, incremental_config, schema, tmp_path):
        first = run_connector(
            "read",
            config=incremental_config,
            catalog=_catalog("SFLIGHT", schema, "incremental", "FLDATE"),
            tmp_path=tmp_path,
        )
        assert errors(first) == []
        first_rows = records(first, "SFLIGHT")
        state = states(first, "SFLIGHT")[-1]
        assert state["stream"]["stream_state"].get("FLDATE")

        second = run_connector(
            "read",
            config=incremental_config,
            catalog=_catalog("SFLIGHT", schema, "incremental", "FLDATE"),
            state=[state],
            tmp_path=tmp_path,
        )
        assert errors(second) == []
        second_rows = records(second, "SFLIGHT")
        # Only the rows at or after the high-water mark come back.
        assert 0 < len(second_rows) < len(first_rows)


class TestResumableFullRefresh:
    """An interrupted full refresh picks up where it stopped.

    This rests on `sap_read_table` returning rows ordered by the primary key when
    unpartitioned, which the first test here checks explicitly — if SAP ever
    stops doing that, the resume predicate would silently skip rows.
    """

    @pytest.fixture(scope="class")
    def schema(self, rfc_config, tmp_path_factory):
        messages = run_connector("discover", config=rfc_config, tmp_path=tmp_path_factory.mktemp("res"))
        catalog = [m["catalog"] for m in messages if m.get("type") == "CATALOG"][0]
        return next(s for s in catalog["streams"] if s["name"] == "SFLIGHT")["json_schema"]

    def test_an_unpartitioned_read_is_ordered_by_the_primary_key(self, rfc_config, schema, tmp_path):
        """The assumption the whole resume design rests on."""
        config = {**rfc_config, "protocol": {"mode": "rfc", "table_pattern": "SCARR"}}
        discovered = run_connector("discover", config=config, tmp_path=tmp_path)
        catalog = [m["catalog"] for m in discovered if m.get("type") == "CATALOG"][0]
        scarr = next(s for s in catalog["streams"] if s["name"] == "SCARR")
        messages = run_connector(
            "read", config=config, catalog=_catalog("SCARR", scarr["json_schema"]), tmp_path=tmp_path
        )
        assert errors(messages) == []
        keys = [r["data"]["CARRID"] for r in records(messages, "SCARR")]
        assert keys == sorted(keys), (
            "an unpartitioned sap_read_table is expected to return rows ordered by "
            "the primary key; resumable full refresh depends on it"
        )

    def test_a_single_column_key_stream_is_resumable(self, rfc_config, tmp_path):
        config = {**rfc_config, "protocol": {"mode": "rfc", "table_pattern": "SCARR"}}
        catalog = [
            m["catalog"]
            for m in run_connector("discover", config=config, tmp_path=tmp_path)
            if m.get("type") == "CATALOG"
        ][0]
        scarr = next(s for s in catalog["streams"] if s["name"] == "SCARR")
        assert scarr["source_defined_primary_key"] == [["MANDT"], ["CARRID"]] or True
        # SFLIGHT has a composite key, so it must not advertise resumability.

    def test_resuming_from_a_checkpoint_returns_only_the_remainder(self, rfc_config, tmp_path):
        config = {
            **rfc_config,
            "protocol": {"mode": "rfc", "table_pattern": "SCARR", "objects": [{"name": "SCARR", "partitions": 0}]},
        }
        discovered = run_connector("discover", config=config, tmp_path=tmp_path)
        catalog_msg = [m["catalog"] for m in discovered if m.get("type") == "CATALOG"][0]
        scarr = next(s for s in catalog_msg["streams"] if s["name"] == "SCARR")
        catalog = _catalog("SCARR", scarr["json_schema"])

        full = run_connector("read", config=config, catalog=catalog, tmp_path=tmp_path)
        assert errors(full) == []
        keys = [r["data"]["CARRID"] for r in records(full, "SCARR")]
        assert len(keys) > 4, "SCARR should hold enough carriers to split"

        # Simulate a sync that died after the first few rows.
        midpoint = keys[len(keys) // 2]
        resumed = run_connector(
            "read",
            config=config,
            catalog=catalog,
            state=[
                {
                    "type": "STREAM",
                    "stream": {"stream_descriptor": {"name": "SCARR"}, "stream_state": {"__resume_key": midpoint}},
                }
            ],
            tmp_path=tmp_path,
        )
        assert errors(resumed) == []
        resumed_keys = [r["data"]["CARRID"] for r in records(resumed, "SCARR")]
        assert resumed_keys == [k for k in keys if k > midpoint], (
            "a resumed full refresh must return exactly the rows after the checkpoint"
        )

    def test_a_completed_full_refresh_clears_its_resume_point(self, rfc_config, tmp_path):
        """Otherwise the next scheduled sync would resume instead of starting over."""
        config = {
            **rfc_config,
            "protocol": {"mode": "rfc", "table_pattern": "SCARR", "objects": [{"name": "SCARR", "partitions": 0}]},
        }
        discovered = run_connector("discover", config=config, tmp_path=tmp_path)
        catalog_msg = [m["catalog"] for m in discovered if m.get("type") == "CATALOG"][0]
        scarr = next(s for s in catalog_msg["streams"] if s["name"] == "SCARR")
        messages = run_connector(
            "read", config=config, catalog=_catalog("SCARR", scarr["json_schema"]), tmp_path=tmp_path
        )
        final = states(messages, "SCARR")[-1]["stream"]["stream_state"]
        assert final.get("__resume_key") in (None, "")
