"""ODP over RFC and ODP over OData: plan construction and state handling."""

from source_sap.protocols.base import SapObject
from source_sap.protocols.odp_odata import OdpODataDriver
from source_sap.protocols.odp_rfc import OdpRfcDriver, subscriber_process_for


def rfc_driver(**protocol):
    return OdpRfcDriver(
        {
            "ashost": "h",
            "sysnr": "00",
            "client": "001",
            "user": "u",
            "password": "p",
            "protocol": {"mode": "odp_rfc", **protocol},
        }
    )


def odata_driver(**protocol):
    return OdpODataDriver(
        {"base_url": "http://sap:50000", "user": "u", "password": "p", "protocol": {"mode": "odp_odata", **protocol}}
    )


class TestSubscriberProcess:
    def test_is_deterministic(self):
        a = subscriber_process_for("conn-1", "ABAP_CDS", "ZV$F")
        assert a == subscriber_process_for("conn-1", "ABAP_CDS", "ZV$F")

    def test_differs_per_object(self):
        assert subscriber_process_for("c", "ABAP_CDS", "A$F") != subscriber_process_for("c", "ABAP_CDS", "B$F")

    def test_fits_sap_field_width(self):
        # RODPS subscriber process is CHAR(32).
        got = subscriber_process_for("a-very-long-airbyte-connection-identifier", "ABAP_CDS", "SOME$LONG$NAME$F")
        assert len(got) <= 32

    def test_is_upper_case_and_alphanumeric(self):
        got = subscriber_process_for("conn/1", "ABAP_CDS", "ZV$F")
        assert got == got.upper()
        assert all(c.isalnum() or c == "_" for c in got)


class TestOdpRfcPlans:
    def _obj(self, **meta):
        return SapObject(
            name="ABAP_CDS/ZV$F",
            json_schema={},
            supports_incremental=True,
            change_mode_field="ODQ_CHANGEMODE",
            meta={"context": "ABAP_CDS", "odp_name": "ZV$F", **meta},
        )

    def test_full_refresh_uses_read_full(self):
        sql = rfc_driver().read_plans(None, self._obj(), incremental=False, state={})[0].sql
        assert "sap_odp_read_full('ABAP_CDS', 'ZV$F'" in sql

    def test_incremental_uses_read_delta_with_the_subscriber_process(self):
        d = rfc_driver()
        plans = d.read_plans(None, self._obj(subscriber_process="AB_X"), incremental=True, state={})
        assert "sap_odp_read_delta('ABAP_CDS', 'ZV$F', 'AB_X')" in plans[0].sql

    def test_delta_is_never_parallelised(self):
        # erpl caps delta fetch to one thread; asking for more races the pointer.
        sql = (
            rfc_driver(threads=8)
            .read_plans(None, self._obj(subscriber_process="AB_X"), incremental=True, state={})[0]
            .sql
        )
        assert "THREADS" not in sql.upper()

    def test_full_refresh_honours_threads(self):
        sql = rfc_driver(threads=8).read_plans(None, self._obj(), incremental=False, state={})[0].sql
        assert "THREADS := 8" in sql

    def test_delta_streams_share_a_concurrency_group(self):
        d = rfc_driver()
        assert d.concurrency_group(self._obj(subscriber_process="AB_X")) == "AB_X"

    def test_state_records_the_subscriber_process(self):
        d = rfc_driver()
        state = d.next_state(None, self._obj(subscriber_process="AB_X"), {})
        assert state["subscriber_process"] == "AB_X"
        assert state["initialized"] is True


class TestOdpODataPlans:
    def _obj(self, **meta):
        return SapObject(
            name="FactsOfZ",
            json_schema={},
            supports_incremental=True,
            change_mode_field="ODQ_CHANGEMODE",
            meta={"entity_set_url": "http://sap:50000/srv/FactsOfZ", "entity_set": "FactsOfZ", **meta},
        )

    def test_read_binds_the_url_as_a_parameter(self):
        plan = odata_driver().read_plans(None, self._obj(), incremental=False, state={})[0]
        assert "odp_odata_read(?" in plan.sql
        assert plan.params[0] == "http://sap:50000/srv/FactsOfZ"

    def test_full_refresh_forces_a_full_load(self):
        sql = odata_driver().read_plans(None, self._obj(), incremental=False, state={})[0].sql
        assert "force_full_load := true" in sql

    def test_incremental_does_not_force_a_full_load(self):
        sql = odata_driver().read_plans(None, self._obj(), incremental=True, state={"delta_token": "D1"})[0].sql
        assert "force_full_load" not in sql

    def test_max_page_size_is_applied(self):
        sql = odata_driver(max_page_size=5000).read_plans(None, self._obj(), incremental=False, state={})[0].sql
        assert "max_page_size := 5000" in sql


class TestOdpODataStateSeeding:
    def test_seed_writes_into_erpl_webs_own_table(self):
        statements = OdpODataDriver.seed_subscription_statements(
            "http://sap:50000/srv/FactsOfZ", "FactsOfZ", "D20260101_1"
        )
        assert any("erpl_web.odp_subscriptions" in sql for sql, _ in statements)

    def test_the_existing_row_is_deleted_before_inserting(self):
        # odp_subscriptions has a PRIMARY KEY *and* a UNIQUE(service_url,
        # entity_set_name); DuckDB refuses INSERT OR REPLACE on a table with
        # more than one unique constraint.
        statements = OdpODataDriver.seed_subscription_statements("http://sap:50000/srv/FactsOfZ", "FactsOfZ", "D1")
        kinds = [sql.strip().split()[0].upper() for sql, _ in statements]
        assert kinds == ["DELETE", "INSERT"]
        assert "OR REPLACE" not in " ".join(sql for sql, _ in statements)

    def test_the_delete_matches_the_unique_key_not_the_id(self):
        # subscription_id is timestamp-prefixed and unstable across runs.
        (delete_sql, delete_params), _ = OdpODataDriver.seed_subscription_statements(
            "http://sap:50000/srv/FactsOfZ", "FactsOfZ", "D1"
        )
        assert "service_url = ?" in delete_sql and "entity_set_name = ?" in delete_sql
        assert list(delete_params) == ["http://sap:50000/srv/FactsOfZ", "FactsOfZ"]

    def test_values_are_never_interpolated(self):
        statements = OdpODataDriver.seed_subscription_statements(
            "http://sap:50000/srv/FactsOfZ", "FactsOfZ", "D20260101_1"
        )
        for sql, _ in statements:
            assert "D20260101_1" not in sql
        assert any("D20260101_1" in list(params) for _, params in statements)
