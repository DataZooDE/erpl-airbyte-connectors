"""DuckDB + ERPL session management.

One :class:`ErplSession` per connector process.  It owns a single DuckDB
database (a *file*, not ``:memory:`` -- erpl_web refuses to keep ODP delta
state in an in-memory database) and hands out thread-local cursors, because a
``DuckDBPyConnection`` is not safe to share across the Concurrent CDK's worker
threads.

Extensions are **loaded, never installed**: the image bakes them in, so a sync
makes no request to get.erpl.io.
"""

from __future__ import annotations

import logging
import os
import tempfile
import threading
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import duckdb

from source_sap.errors import config_error, traced
from source_sap.retry import retry_transient

logger = logging.getLogger("airbyte")

# Where the Dockerfile pre-warms the unpacked ERPL extensions.
DEFAULT_EXTENSION_DIR = os.environ.get("ERPL_EXTENSION_DIR", "/airbyte/duckdb_extensions")

ALL_EXTENSIONS = ("erpl_rfc", "erpl_bics", "erpl_odp", "erpl_web")

# Named secret used for every SAP RFC call.  Several ERPL functions
# (`sap_show_tables`, for one) take no `secret` argument and fall back to the
# best-matching secret in scope, so the session creates exactly one.
RFC_SECRET_NAME = "airbyte_sap"
HTTP_SECRET_NAME = "airbyte_sap_http"

# Field name in config -> field name in the `sap_rfc` secret.
_RFC_FIELDS: dict[str, str] = {
    "ashost": "ashost",
    "sysnr": "sysnr",
    "mshost": "mshost",
    "msserv": "msserv",
    "sysid": "sysid",
    "group": "group",
    "client": "client",
    "user": "user",
    "password": "passwd",
    "lang": "lang",
    "snc_mode": "snc_mode",
    "snc_sso": "snc_sso",
    "snc_qop": "snc_qop",
    "snc_myname": "snc_myname",
    "snc_partnername": "snc_partnername",
    "snc_lib": "snc_lib",
    "saprouter": "saprouter",
    "gwhost": "gwhost",
    "gwserv": "gwserv",
    "codepage": "codepage",
}


@dataclass(frozen=True)
class SapRfcCredentials:
    """Values for a `CREATE SECRET ... (TYPE sap_rfc, ...)` statement."""

    params: dict[str, str]

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> SapRfcCredentials:
        params: dict[str, str] = {}
        for cfg_key, secret_key in _RFC_FIELDS.items():
            value = config.get(cfg_key)
            if value is None:
                continue
            text = str(value).strip()
            if text:  # omit blanks entirely; SAP treats "" differently from absent
                params[secret_key] = text
        if "ashost" not in params and "mshost" not in params:
            raise config_error(
                "No SAP application server configured. Set either 'ashost' (direct logon) "
                "or 'mshost' together with 'sysid' and 'group' (load-balanced logon)."
            )
        if "ashost" in params and "sysnr" not in params:
            raise config_error("A direct SAP logon needs a system number ('sysnr'), e.g. '00'.")
        return cls(params=params)

    def create_secret_sql(self, name: str = RFC_SECRET_NAME) -> tuple[str, dict[str, str]]:
        """Return `(sql, params)`. Credentials travel as bound parameters only."""
        assignments = ", ".join(f"{key.upper()} ${key}" for key in self.params)
        sql = f"CREATE OR REPLACE SECRET {name} (TYPE sap_rfc, {assignments})"
        return sql, dict(self.params)


@dataclass(frozen=True)
class HttpBasicCredentials:
    """Values for the `http_basic` secret erpl_web resolves by URL scope."""

    username: str
    password: str
    scope: str

    def create_secret_sql(self, name: str = HTTP_SECRET_NAME) -> tuple[str, dict[str, str]]:
        sql = f"CREATE OR REPLACE SECRET {name} (TYPE http_basic, SCOPE $scope, USERNAME $username, PASSWORD $password)"
        return sql, {"scope": self.scope, "username": self.username, "password": self.password}


#: Upper bound on DuckDB's thread pool for this process.
#:
#: These threads are not doing CPU work: each one is blocked on an RFC call, so
#: the budget is really "how many SAP round trips are in flight". Measured on a
#: 55-column, 164,673-row table, throughput rises near-linearly with it -- 1,479
#: rows/s at 1 thread, 8,740 at 8, 11,720 at 16, 12,919 at 32 -- so the last
#: doubling buys 10% for twice the SAP-side load. erpl caches at most 16 RFC
#: connections itself and its source cites a ~4x effective concurrency ceiling
#: at the SAP gateway, which is where this number comes from. See
#: docs/performance.md.
MAX_DUCKDB_THREADS = 16


@dataclass(frozen=True)
class SessionSettings:
    num_workers: int = 4
    cpu_count: int = field(default_factory=lambda: os.cpu_count() or 4)

    @property
    def duckdb_threads(self) -> int:
        """How many SAP round trips this process may have in flight.

        Deliberately *not* divided by the Airbyte worker count. Every stream
        shares one DuckDB instance (see this module's docstring), and DuckDB's
        scheduler already shares its threads across the queries running on it --
        so dividing did not prevent oversubscription, it starved the process:
        a single-stream sync measured 3.9x slower at `concurrency: 16` than at
        1, for a setting that is supposed to govern how many *streams* run at
        once.
        """
        return max(1, min(self.cpu_count, MAX_DUCKDB_THREADS))


class ErplSession:
    """Owns the DuckDB database and the per-thread cursors."""

    def __init__(
        self,
        config: Mapping[str, Any],
        extensions: Sequence[str] = ALL_EXTENSIONS,
        settings: SessionSettings | None = None,
        database_path: str | None = None,
        extension_dir: str | None = None,
    ) -> None:
        self._config = config
        self._extensions = tuple(extensions)
        self._settings = settings or SessionSettings()
        self._extension_dir = extension_dir or DEFAULT_EXTENSION_DIR
        self._tempdir: tempfile.TemporaryDirectory[str] | None = None
        if database_path is None:
            # erpl_web stores ODP delta state in a persistent database and raises
            # on :memory:. The file is scratch -- Airbyte state is the real source
            # of truth -- so a temp dir is enough.
            self._tempdir = tempfile.TemporaryDirectory(prefix="airbyte-sap-")
            database_path = os.path.join(self._tempdir.name, "state.duckdb")
        self._database_path = database_path
        self._local = threading.local()
        self._cursors: list[duckdb.DuckDBPyConnection] = []
        self._lock = threading.Lock()
        self._root = self._connect_root()

    # ---- construction helpers -------------------------------------------------

    @staticmethod
    def bootstrap_statements(extensions: Iterable[str]) -> list[str]:
        statements = [f"LOAD {ext}" for ext in extensions]
        statements.append("SET erpl_telemetry_enabled = false")
        return statements

    @retry_transient()
    def _connect_root(self) -> duckdb.DuckDBPyConnection:
        """Opening the session reaches SAP, so a busy system is worth waiting out."""
        duckdb_config = {
            "allow_unsigned_extensions": "true",
            "extension_directory": self._extension_dir,
            "threads": str(self._settings.duckdb_threads),
        }
        try:
            con = duckdb.connect(self._database_path, config=duckdb_config)
        except Exception as exc:  # pragma: no cover - environment failure
            raise traced("Could not open the embedded DuckDB database", exc) from exc
        self._bootstrap(con)
        return con

    def _bootstrap(self, con: duckdb.DuckDBPyConnection) -> None:
        for statement in self.bootstrap_statements(self._extensions):
            try:
                con.execute(statement)
            except Exception as exc:
                raise traced(f"Failed to initialise the ERPL extensions ({statement})", exc) from exc
        self._create_secrets(con)

    def _create_secrets(self, con: duckdb.DuckDBPyConnection) -> None:
        if any(ext in self._extensions for ext in ("erpl_rfc", "erpl_bics", "erpl_odp")):
            creds = SapRfcCredentials.from_config(self._config)
            sql, params = creds.create_secret_sql()
            try:
                con.execute(sql, params)
            except Exception as exc:
                raise traced("Could not register the SAP RFC credentials", exc) from exc
        if "erpl_web" in self._extensions:
            http = self._http_credentials()
            if http is not None:
                sql, params = http.create_secret_sql()
                try:
                    con.execute(sql, params)
                except Exception as exc:
                    raise traced("Could not register the SAP HTTP credentials", exc) from exc

    def _http_credentials(self) -> HttpBasicCredentials | None:
        base_url = (self._config.get("base_url") or "").strip()
        user = self._config.get("user")
        password = self._config.get("password")
        if not (base_url and user and password):
            return None
        scope = base_url if base_url.endswith("/") else base_url + "/"
        return HttpBasicCredentials(username=str(user), password=str(password), scope=scope)

    # ---- usage ----------------------------------------------------------------

    @property
    def root(self) -> duckdb.DuckDBPyConnection:
        return self._root

    def cursor(self) -> duckdb.DuckDBPyConnection:
        """A cursor private to the calling thread, sharing one database."""
        existing = getattr(self._local, "cursor", None)
        if existing is not None:
            return existing
        cursor = self._root.cursor()
        with self._lock:
            self._cursors.append(cursor)
        self._local.cursor = cursor
        return cursor

    def disposable(self) -> duckdb.DuckDBPyConnection:
        """A throwaway connection for calls that can poison the database.

        `odp_odata_show` raises an INTERNAL error against some SAP Gateway
        releases, which invalidates the whole DuckDB instance.
        """
        con = duckdb.connect(
            ":memory:",
            config={
                "allow_unsigned_extensions": "true",
                "extension_directory": self._extension_dir,
            },
        )
        self._bootstrap(con)
        return con

    def close(self) -> None:
        """erpl_web asserts if its connection is torn down at interpreter exit."""
        with self._lock:
            cursors, self._cursors = self._cursors, []
        for cursor in cursors:
            try:
                cursor.close()
            except Exception:  # pragma: no cover - best effort
                logger.debug("Ignoring error while closing a DuckDB cursor", exc_info=True)
        try:
            self._root.close()
        except Exception:  # pragma: no cover - best effort
            logger.debug("Ignoring error while closing the DuckDB database", exc_info=True)
        if self._tempdir is not None:
            self._tempdir.cleanup()
            self._tempdir = None

    def __enter__(self) -> ErplSession:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
