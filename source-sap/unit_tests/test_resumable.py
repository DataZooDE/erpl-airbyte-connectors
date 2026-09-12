"""Resumable full refresh for RFC.

A full refresh of a six-figure table that dies halfway currently starts over.
ERPL exposes no row offset on `sap_read_table`, but an unpartitioned scan returns
rows ordered by the primary key (verified against a live system: SFLIGHT and
SCARR both come back sorted), so the last key seen is a usable resume point.

The ordering only holds for an unpartitioned scan, so a partitioned stream is
deliberately not resumable.
"""

from source_sap.protocols.base import SapObject
from source_sap.protocols.rfc import RfcDriver

CONN = {"ashost": "h", "sysnr": "00", "client": "001", "user": "u", "password": "p"}


def driver(**obj):
    return RfcDriver({**CONN, "protocol": {"mode": "rfc", "objects": [{"name": "T", **obj}]}})


def sap_object(primary_key=None, **meta):
    return SapObject(
        name="T",
        json_schema={},
        primary_key=primary_key,
        meta={"table": "T", "resume_key": (primary_key or [["K"]])[0][0], **meta},
    )


class TestResumability:
    def test_a_single_column_key_and_no_partitions_is_resumable(self):
        assert driver().is_resumable(sap_object([["K"]]), partitions=0)

    def test_a_partitioned_stream_is_not_resumable(self):
        # Partitioned reads return rows in an unspecified order, so a resume key
        # would skip whatever a slower worker had not yet emitted.
        assert not driver().is_resumable(sap_object([["K"]]), partitions=8)

    def test_a_composite_key_is_not_resumable(self):
        # Discovery leaves resume_key unset when more than one non-client key field remains.
        assert not driver().is_resumable(sap_object([["A"], ["B"]], resume_key=None), partitions=0)

    def test_a_table_with_no_key_is_not_resumable(self):
        assert not driver().is_resumable(sap_object(None, resume_key=None), partitions=0)


class TestResumePredicate:
    def _sql(self, state, **obj):
        d = driver(partitions=0, **obj)
        return d.read_plans(None, sap_object([["K"]]), incremental=False, state=state)[0].sql

    def test_no_state_reads_from_the_beginning(self):
        assert "FILTER" not in self._sql({})

    def test_a_resume_key_becomes_a_greater_than_predicate(self):
        assert "FILTER := 'K > ''ABC'''" in self._sql({"__resume_key": "ABC"})

    def test_the_resume_key_is_escaped(self):
        sql = self._sql({"__resume_key": "a' OR '1'='1"})
        assert sql.count("'") % 2 == 0

    def test_a_user_filter_and_the_resume_predicate_combine(self):
        sql = self._sql({"__resume_key": "ABC"}, filter="X = 'Y'")
        assert " AND " in sql and "K > ''ABC''" in sql

    def test_a_partitioned_stream_ignores_a_stale_resume_key(self):
        d = driver(partitions=8)
        sql = d.read_plans(None, sap_object([["K"]]), incremental=False, state={"__resume_key": "ABC"})[0].sql
        assert "K >" not in sql


class TestResumeCursor:
    def _cursor(self, obj):
        from airbyte_cdk.sources.connector_state_manager import ConnectorStateManager
        from airbyte_cdk.sources.message import InMemoryMessageRepository

        from source_sap.cursors import ResumeKeyCursor

        repo, mgr = InMemoryMessageRepository(), ConnectorStateManager()
        return ResumeKeyCursor("T", None, repo, mgr, "K", {}), repo

    def test_it_tracks_the_highest_key_seen(self):
        from unittest.mock import MagicMock

        cursor, _ = self._cursor(sap_object([["K"]]))
        for value in ["A", "C", "B"]:
            record = MagicMock()
            record.data = {"K": value}
            cursor.observe(record)
        assert cursor.state["__resume_key"] == "C"

    def test_completing_the_stream_clears_the_resume_point(self):
        """A finished full refresh must start from the beginning next time."""
        from unittest.mock import MagicMock

        cursor, repo = self._cursor(sap_object([["K"]]))
        record = MagicMock()
        record.data = {"K": "A"}
        cursor.observe(record)
        cursor.ensure_at_least_one_state_emitted()
        (message,) = list(repo.consume_queue())
        assert message.state.stream.stream_state.__dict__.get("__resume_key") in (None, "")

    def test_an_interrupted_stream_keeps_the_resume_point(self):
        from unittest.mock import MagicMock

        cursor, repo = self._cursor(sap_object([["K"]]))
        record = MagicMock()
        record.data = {"K": "A"}
        cursor.observe(record)
        cursor.close_partition(MagicMock())
        (message,) = list(repo.consume_queue())
        assert message.state.stream.stream_state.__dict__["__resume_key"] == "A"


class TestClientFieldIsNotPartOfTheResumeKey:
    """Almost every SAP table is client-dependent, so MANDT is in almost every key.

    Treating it as part of the key would make resumability apply to virtually no
    table. It is constant within a sync -- a connection names exactly one client
    -- so the resume point is the remaining key field.
    """

    def _fields(self, *specs):
        return [
            {"technical_name": name, "abap_type": sap_type, "length": 3, "decimals": 0, "key": True}
            for name, sap_type in specs
        ]

    def test_a_client_plus_one_key_field_is_resumable(self):
        from source_sap.types import resume_key_field

        assert resume_key_field(self._fields(("MANDT", "CLNT"), ("CARRID", "CHAR"))) == "CARRID"

    def test_a_client_plus_two_key_fields_is_not(self):
        from source_sap.types import resume_key_field

        assert resume_key_field(self._fields(("MANDT", "CLNT"), ("CARRID", "CHAR"), ("CONNID", "NUMC"))) is None

    def test_a_client_independent_single_key_is_resumable(self):
        from source_sap.types import resume_key_field

        assert resume_key_field(self._fields(("TABNAME", "CHAR"))) == "TABNAME"

    def test_the_client_field_is_recognised_by_its_ddic_type_not_its_name(self):
        from source_sap.types import resume_key_field

        # A client field is any key field of DDIC type CLNT, whatever it is called.
        assert resume_key_field(self._fields(("MANDANT", "CLNT"), ("ID", "CHAR"))) == "ID"

    def test_a_client_only_key_is_not_resumable(self):
        from source_sap.types import resume_key_field

        assert resume_key_field(self._fields(("MANDT", "CLNT"))) is None

    def test_no_key_at_all_is_not_resumable(self):
        from source_sap.types import resume_key_field

        assert resume_key_field([]) is None

    def test_non_key_fields_are_ignored(self):
        from source_sap.types import resume_key_field

        fields = self._fields(("MANDT", "CLNT"), ("CARRID", "CHAR"))
        fields.append({"technical_name": "TEXT", "abap_type": "CHAR", "length": 20, "decimals": 0, "key": False})
        assert resume_key_field(fields) == "CARRID"
