# The erpl extensions are published for linux_amd64 only -- there is no arm64 build.
FROM --platform=linux/amd64 docker.io/airbyte/python-connector-base:4.1.1@sha256:a268b44c733ae699a60f5fbc06a324945dba98945c6e2ab7f8609f2f895b0d28 AS extensions

ARG DUCKDB_VERSION=v1.5.5
ARG ERPL_REPO=http://get.erpl.io
ENV ERPL_EXTENSION_DIR=/airbyte/duckdb_extensions

USER root
RUN pip install --no-cache-dir "duckdb==1.5.5"
COPY bin/fetch-extensions.sh /tmp/fetch-extensions.sh
# Downloads erpl + erpl_web, unpacks the trampoline's payload (erpl_rfc, erpl_bics,
# erpl_odp and the SAP/ICU shared objects), then deletes the 180 MB trampoline.
RUN DUCKDB_VERSION="${DUCKDB_VERSION}" ERPL_REPO="${ERPL_REPO}" \
    bash /tmp/fetch-extensions.sh "${ERPL_EXTENSION_DIR}" && rm /tmp/fetch-extensions.sh


FROM --platform=linux/amd64 docker.io/airbyte/python-connector-base:4.1.1@sha256:a268b44c733ae699a60f5fbc06a324945dba98945c6e2ab7f8609f2f895b0d28

ARG DUCKDB_VERSION=v1.5.5
ENV ERPL_EXTENSION_DIR=/airbyte/duckdb_extensions \
    LD_LIBRARY_PATH=/airbyte/duckdb_extensions/v1.5.5/linux_amd64 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/usr/local

COPY --from=ghcr.io/astral-sh/uv:0.12.10 /uv /usr/local/bin/uv

WORKDIR /airbyte/integration_code
USER root

# Dependencies first, so a source-only change does not re-resolve them.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-install-project --no-dev

COPY source_sap ./source_sap
COPY main.py ./
RUN uv sync --frozen --no-dev

COPY --from=extensions /airbyte/duckdb_extensions /airbyte/duckdb_extensions
RUN chown -R airbyte:airbyte /airbyte

USER airbyte
ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]
