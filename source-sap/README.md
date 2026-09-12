# Airbyte Source: SAP

Reads data out of SAP systems through the [ERPL](https://erpl.io) DuckDB extensions.
One connector covers four access protocols:

| Mode | Extension | What it reads |
|---|---|---|
| `rfc` | `erpl_rfc` | SAP tables and CDS views via `RFC_READ_TABLE` |
| `bics` | `erpl_bics` | SAP BW cubes and BEx queries via BICS |
| `odp_rfc` | `erpl_odp` | ODP providers (BW, ABAP_CDS, SAPI, HANA), full and delta |
| `odp_odata` | `erpl_web` | ODP over the SAP Gateway OData protocol, full and delta |

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
uv run pytest unit_tests -q                    # pure unit tests
uv run pytest integration_tests -q             # needs a live SAP system
uv run pytest e2e -q                           # full connector runs, no mocks
```

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
