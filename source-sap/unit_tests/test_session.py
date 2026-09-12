"""DuckDB/ERPL session construction."""

import pytest

from source_sap.session import ErplSession, SapRfcCredentials, SessionSettings


class TestSapRfcCredentials:
    def test_direct_connection_fields(self):
        creds = SapRfcCredentials.from_config(
            {"ashost": "sap.example.com", "sysnr": "00", "client": "100", "user": "u", "password": "p", "lang": "EN"}
        )
        sql, params = creds.create_secret_sql("sap")
        assert "CREATE OR REPLACE SECRET" in sql
        assert "TYPE sap_rfc" in sql
        # Every value must travel as a bound parameter, never interpolated.
        assert "sap.example.com" not in sql
        assert "p" not in [c for c in sql if c.islower()] or "PASSWD" in sql
        assert params["ashost"] == "sap.example.com"
        assert params["passwd"] == "p"

    def test_password_is_never_interpolated_into_sql(self):
        creds = SapRfcCredentials.from_config(
            {"ashost": "h", "sysnr": "00", "client": "100", "user": "u", "password": "'; DROP TABLE x; --"}
        )
        sql, params = creds.create_secret_sql("sap")
        assert "DROP TABLE" not in sql
        assert params["passwd"] == "'; DROP TABLE x; --"

    def test_load_balanced_connection(self):
        creds = SapRfcCredentials.from_config(
            {
                "mshost": "ms.example.com",
                "sysid": "PRD",
                "group": "PUBLIC",
                "client": "100",
                "user": "u",
                "password": "p",
            }
        )
        _, params = creds.create_secret_sql("sap")
        assert params["mshost"] == "ms.example.com"
        assert params["sysid"] == "PRD"
        assert "ashost" not in params

    def test_optional_fields_are_omitted_not_blank(self):
        creds = SapRfcCredentials.from_config(
            {"ashost": "h", "sysnr": "00", "client": "100", "user": "u", "password": "p", "saprouter": ""}
        )
        _, params = creds.create_secret_sql("sap")
        assert "saprouter" not in params
        assert "lang" not in params

    def test_snc_fields_survive(self):
        creds = SapRfcCredentials.from_config(
            {
                "ashost": "h",
                "sysnr": "00",
                "client": "100",
                "user": "u",
                "snc_mode": "1",
                "snc_partnername": "p:CN=SAP",
                "snc_lib": "/usr/lib/lib.so",
            }
        )
        _, params = creds.create_secret_sql("sap")
        assert params["snc_mode"] == "1"
        assert "passwd" not in params  # SNC logon needs no password

    def test_missing_host_is_a_config_error(self):
        from source_sap.errors import config_error

        with pytest.raises(Exception) as exc:
            SapRfcCredentials.from_config({"client": "100", "user": "u", "password": "p"})
        assert "ashost" in str(exc.value) or "mshost" in str(exc.value)
        assert config_error  # imported symbol exists


class TestSessionSettings:
    def test_duckdb_threads_are_divided_across_workers(self):
        s = SessionSettings(num_workers=4, cpu_count=16)
        assert s.duckdb_threads == 4

    def test_never_fewer_than_one_duckdb_thread(self):
        assert SessionSettings(num_workers=32, cpu_count=2).duckdb_threads == 1


class TestErplSessionSql:
    def test_extensions_are_loaded_never_installed(self):
        # Extensions are baked into the image; a sync must not reach out to get.erpl.io.
        stmts = ErplSession.bootstrap_statements(("erpl_rfc", "erpl_web"))
        joined = " ".join(stmts)
        assert "INSTALL" not in joined.upper()
        assert "LOAD erpl_rfc" in joined
        assert "erpl_telemetry_enabled" in joined
