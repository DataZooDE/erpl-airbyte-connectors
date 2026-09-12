"""Fixes from crew round 2."""

from unittest.mock import MagicMock

from airbyte_cdk.models import SyncMode

from source_sap.protocols.base import SapObject
from source_sap.protocols.rfc import RfcDriver

CONN = {"ashost": "h", "sysnr": "00", "client": "001", "user": "u", "password": "p"}


def _driver(**protocol):
    return RfcDriver({**CONN, "protocol": {"mode": "rfc", **protocol}})


def _obj(resume_key="CARRID", **meta):
    return SapObject(
        name="T",
        json_schema={"type": "object", "properties": {}},
        primary_key=[["MANDT"], ["CARRID"]],
        meta={"table": "T", "resume_key": resume_key, **meta},
    )


def _stream(driver, obj, incremental=False):
    from airbyte_cdk.sources.connector_state_manager import ConnectorStateManager
    from airbyte_cdk.sources.message import InMemoryMessageRepository

    from source_sap.cursors import NoStateCursor
    from source_sap.streams import build_stream

    cursor = NoStateCursor("T", None, InMemoryMessageRepository(), ConnectorStateManager())
    return build_stream(MagicMock(), driver, obj, cursor, incremental=incremental, state={})


class TestResumableStreamsSaySo:
    """A full-refresh stream that does not advertise is_resumable never gets its
    state back from the platform, so the resume machinery would never fire."""

    def test_a_resumable_full_refresh_stream_advertises_it(self):
        stream = _stream(_driver(), _obj()).as_airbyte_stream()
        assert stream.is_resumable is True
        assert stream.supported_sync_modes == [SyncMode.full_refresh]

    def test_a_partitioned_stream_does_not(self):
        # Partitioned reads are unordered, so there is no safe resume point.
        driver = _driver(objects=[{"name": "T", "partitions": 8}])
        assert _stream(driver, _obj()).as_airbyte_stream().is_resumable is not True

    def test_a_stream_without_a_usable_key_does_not(self):
        assert _stream(_driver(), _obj(resume_key=None)).as_airbyte_stream().is_resumable is not True

    def test_an_incremental_stream_still_advertises_it(self):
        obj = SapObject(name="T", json_schema={}, supports_incremental=True, meta={"table": "T"})
        stream = _stream(_driver(), obj, incremental=True).as_airbyte_stream()
        assert SyncMode.incremental in stream.supported_sync_modes
        assert stream.is_resumable is True


class TestPartitionsAlwaysGovernTheSql:
    """One number must drive the SQL, resumability and the resume predicate.

    With PARTITIONS omitted from the query, an absent setting meant one thing to
    `is_resumable` and another to what SAP actually did.
    """

    def test_the_partition_count_is_always_stated(self):
        sql = _driver().read_plans(None, _obj(), incremental=False, state={})[0].sql
        assert "PARTITIONS := 0" in sql

    def test_a_configured_count_is_used(self):
        driver = _driver(objects=[{"name": "T", "partitions": 4}])
        sql = driver.read_plans(None, _obj(), incremental=False, state={})[0].sql
        assert "PARTITIONS := 4" in sql

    def test_the_sql_and_resumability_agree(self):
        for partitions, resumable in ((0, True), (4, False)):
            driver = _driver(objects=[{"name": "T", "partitions": partitions}])
            sql = driver.read_plans(None, _obj(), incremental=False, state={})[0].sql
            assert f"PARTITIONS := {partitions}" in sql
            assert driver.is_resumable(_obj()) is resumable


class TestOdpProbeIsNotGatedByTheSkipSetting:
    """With skip_unchanged off, the probe used to happen after the read, which is
    the race the probe exists to avoid."""

    def _odp(self, **protocol):
        from source_sap.protocols.odp_rfc import OdpRfcDriver

        return OdpRfcDriver(
            {**CONN, "protocol": {"mode": "odp_rfc", "context": "BW", "objects": [{"name": "N"}], **protocol}}
        )

    def _odp_obj(self):
        return SapObject(
            name="BW/N",
            json_schema={},
            supports_incremental=True,
            meta={"context": "BW", "odp_name": "N", "subscriber_process": "AB_X"},
        )

    def _session(self, *timestamps):
        session = MagicMock()
        session.cursor.return_value.execute.return_value.fetchone.side_effect = [("N", t) for t in timestamps]
        return session

    def test_the_probe_runs_even_when_skipping_is_disabled(self):
        session = self._session(20260101120000.0, 20260101130000.0)
        driver, obj = self._odp(skip_unchanged=False), self._odp_obj()
        plans = driver.read_plans(
            session,
            obj,
            incremental=True,
            state={"subscriber_process": "AB_X", "initialized": True, "last_modified": "20260101120000.0"},
        )
        assert len(plans) == 1, "skipping is off, so it must read"
        state = driver.next_state(session, obj, {"subscriber_process": "AB_X"})
        # The value recorded is the one from before the read, not a later one.
        assert state["last_modified"] == "20260101120000.0"
