# Developing the connector

Building, testing and releasing `source-sap`. For what the connector does, see
the [README](../README.md).

## Local development

Dependencies are managed with [uv](https://docs.astral.sh/uv/).

```bash
uv sync                       # create .venv and install everything
uv run pytest unit_tests -q   # fast tests, no SAP needed
uv run source-sap spec
```

The ERPL extensions are **baked into the image** rather than installed at sync
time. For local runs, point `ERPL_EXTENSION_DIR` at a directory holding them:

```bash
./bin/fetch-extensions.sh ./.erpl        # downloads + pre-warms the extensions
export ERPL_EXTENSION_DIR="$PWD/.erpl"
export LD_LIBRARY_PATH="$ERPL_EXTENSION_DIR/v1.5.5/linux_amd64"
```

## Supply chain

The ERPL extensions are unsigned native code. `bin/fetch-extensions.sh` downloads
them over HTTPS and verifies them against the checksums pinned in
`bin/checksums.txt`, failing the build on a mismatch or an unpinned artifact.
When moving to a new ERPL or DuckDB version:

```bash
ERPL_ALLOW_UNPINNED=1 ./bin/fetch-extensions.sh ./.erpl   # prints the digests
# paste them into bin/checksums.txt
```

## Tests

```bash
uv run pytest unit_tests -q                        # pure unit tests, no SAP
uv run pytest e2e -q -m "not slow"                 # full connector runs, no mocks
uv run pytest e2e -q -m slow                       # six-figure extracts
./bin/build-image.sh
uv run pytest integration_tests -q \
    --connector-image datazoo/source-sap:dev       # Airbyte's standard tests
```

`integration_tests/` runs Airbyte's own standard connector suite. It needs
`secrets/config.json`, which `./bin/write-secrets.sh` generates from the same
environment (pointing `ashost` at the Docker bridge gateway, so the same file
works from the host and from inside the image). Pass `--connector-image` so the
suite uses an image built by `bin/build-image.sh`: without it the CDK generates
its own Dockerfile, which cannot bake in the ERPL extensions.

Integration and e2e tests talk to a real SAP system — by default the
[ABAP Platform Trial](https://hub.docker.com/r/sapse/abap-platform-trial) running
locally. They use the same environment contract as the erpl repository:

```bash
export ERPL_SAP_ASHOST=localhost ERPL_SAP_SYSNR=00 ERPL_SAP_CLIENT=001 \
       ERPL_SAP_USER=DEVELOPER ERPL_SAP_PASSWORD='ABAPtr2023#00' ERPL_SAP_LANG=EN \
       ERPL_SAP_BASE_URL=http://localhost:50000
```

ODP cases are gated separately, and the variables must be **absent** rather than
empty to skip cleanly:

```bash
ERPL_SAP_ODP_CONTEXT=ABAP_CDS ERPL_SAP_ODP_NAME='ZJRODPVSQL$F' \
ERPL_SAP_ODP_ODATA_URL='http://localhost:50000/sap/opu/odata/sap/Z_ODP_DL2_SRV/FactsOfZJRODPVSQL' \
  uv run pytest e2e -q
```

## Performance

`./bin/benchmark.py` measures replication throughput end to end — the connector
as a subprocess, RECORD messages against wall-clock time — across the
partitioning and threading matrix. Writes `docs/performance-raw.md`; the prose interpretation is
[performance.md](performance.md), which is written by hand.

```bash
./bin/benchmark.py --list
./bin/benchmark.py --repeat 3
```

## Building the image

```bash
./bin/build-image.sh            # builds linux/amd64 with the extensions baked in
```

`erpl` publishes no `linux_arm64` binary, so the image is amd64-only.

## Licence

Business Source License 1.1 — see [LICENSE](../LICENSE). Production use is
permitted, but not offering the connector to third parties on a hosted or
embedded basis. The licence converts to MPL 2.0 five years after publication.
The ERPL extensions it loads are licensed the same way.

## Reference systems

The e2e suite is written against the ABAP Platform Trial. On that system:

| Variable | Value |
|---|---|
| `ERPL_SAP_ODP_CONTEXT` | `ABAP_CDS` |
| `ERPL_SAP_ODP_NAME` | `ZJRODPVSQL$F` (a delta-capable CDS view; see the erpl-web docs for provisioning) |
| `ERPL_SAP_ODP_ODATA_URL` | `http://localhost:50000/sap/opu/odata/sap/Z_ODP_DL2_SRV/FactsOfZJRODPVSQL` |
| `ERPL_SAP_BICS_CUBE` | `0D_NW_C01` (ships with the trial) |

`./bin/test-e2e.sh` fills in the connection variables and runs the suite.
