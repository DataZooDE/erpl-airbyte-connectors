# Submitting `source-sap` to the Airbyte OSS registry

A measured readiness assessment, not an estimate. Everything below was produced
by running Airbyte's own tooling against this connector, or by reading the code
that will run it. Claims that came from review rather than execution are marked.

## Current state

Measured on a clean checkout (`git archive HEAD`), because the checks walk the
directory on disk and a local `secrets/config.json` otherwise shows up as an
HTTPS offender that no checkout would have:

**2 failed, 16 passed, 12 skipped, 0 errored — down from 9 failed and 1 errored.**

| Remaining failure | Status |
|---|---|
| `Connectors must be licensed under MIT or Elv2` | Out of scope by instruction; see the last section. |
| `Python connectors must not use a Dockerfile` | Waiting on one thing: publishing `erpl-extensions` to PyPI. |

The Dockerfile is the last technical item and it is *coupled*, not forgotten.
Deleting it is a two-line change, but until the wheel is on PyPI and declared as
a dependency, an image built from Airbyte's template would contain no ERPL
extensions at all. The order is: publish the wheel, add the dependency, relock,
delete the Dockerfile, switch `bin/build-image.sh` and CI to
`airbyte-cdk image build`.

What was done, and what it cost:

- **Packaging** converted from uv to legacy Poetry, the version Airbyte pins
  (1.8.5). This cleared four failures and the one error at once.
- **`[tool.poe]`** added — invisible to the QA checks, but every Airbyte CI step
  is a poe task and none of them run without it.
- **`erpl-extensions`**, a new package in `packages/`, carries the 194 MB payload
  as a platform-tagged wheel (72 MB compressed, one file, under PyPI's limit).
  Verified end to end: with the wheel and the connector pip-installed and a bare
  environment (`env -i`), `check` against the live system reports *Connected to
  SAP via RFC (PONG)*.
- **Extension loading** is resolved from code, preferring the sidecar package,
  with the SAP and ICU shared objects preloaded via `RTLD_GLOBAL` — because the
  generated image sets no `ENV` and `LD_LIBRARY_PATH` cannot be set after start.
- **0.1.0**, `airbyte/source-sap`, `icon.svg`, PyPI declaration, HTTPS comments,
  `build/` untracked.
- **Test contract**: credentialed acceptance categories bypassed with reasons,
  `integrationTests` dropped, and one image test that fails when the extensions
  are missing — proven to fail, not assumed to.
- **Documentation**: `sap.md` stands alone — no relative links out of the file,
  a Cloud/Open-Source split, a data-type map, an inlined `S_RFC` table and a
  troubleshooting table.

Still open, and not blocking the above: the arm64 question (Airbyte publishes
`linux/amd64,linux/arm64`; ERPL builds only amd64), and the three questions for
Airbyte's maintainers below.

## How to reproduce this assessment

Airbyte's QA checks ship as a public PyPI package:

```bash
uv tool install --python 3.13 airbyte-internal-ops     # 0.105.0 at time of writing

# The checks expect a monorepo layout, so build one out of symlinks:
mkdir -p /tmp/qa/airbyte-integrations/connectors /tmp/qa/docs/integrations/sources
ln -s "$PWD/source-sap" /tmp/qa/airbyte-integrations/connectors/source-sap
cp docs/integrations/sources/sap.md /tmp/qa/docs/integrations/sources/
cd /tmp/qa && airbyte-ops local connector qa --name source-sap
```

The CLI aborts partway for an unpublished connector: `Connector.is_released`
queries the live registry and raises `KeyError: 'Nones'`. To see every check,
drive them directly with that property stubbed to `False` — the packaging,
security and asset checks run after the point where the CLI stops.

**Result: 9 failed, 1 errored, 8 passed, 12 skipped, of 30 checks.**

The twelve skips are the structural documentation checks. They apply only at
`ab_internal.sl >= 300`; a community connector defaults to 100. They are skipped,
not satisfied — a human reviewer still reads the page.

## This is a new connector, and that settles two questions

Definition ID `54427944-6368-408d-a074-cf6d556bda0f` is **not** among the 600
sources in `https://connectors.airbyte.com/files/registries/v0/oss_registry.json`.

- **Ship 0.1.0, not 1.0.0.** The metadata validator requires a
  `releases.breakingChanges` entry for any `X.0.0`, regardless of release history.
  Adding one activates three further checks, including a migration guide that *is*
  enforced at sl=100. 498 of the 600 registry sources are on 0.x.
- **The image must be `airbyte/source-sap`.** All 600 registry entries use the
  `airbyte/` namespace; Airbyte's pipeline builds and pushes with Airbyte's
  credentials. `datazoo/source-sap` passes every QA check and fails at publish.

Two SAP connectors already exist in the registry — `SAP Fieldglass` and
`SAP HANA` — so `name: SAP` is broad enough that a reviewer may ask for something
more specific.

## The blockers, in dependency order

### 1. Packaging: Poetry, not uv (root blocker)

Airbyte's CI pins **Poetry 1.8.5**, which predates PEP 621 support: it reads only
`[tool.poetry]` and does not understand `[project]`, `[dependency-groups]`,
`license-files`, or `[project.scripts]`. Every QA check that touches packaging
parses `pyproject["tool"]["poetry"]`, which is why three failures and the one
error are all symptoms of this single gap:

| Symptom | Real cause |
|---|---|
| "poetry.lock file is missing" | no Poetry |
| "missing license in pyproject.toml" | reads `[tool.poetry].license` |
| "Version field is missing" | reads `[tool.poetry].version` |
| CDK-tag check raises `KeyError: 'poetry'` | reads `[tool.poetry].dependencies` |

A hybrid pyproject does not help: Poetry 1.8.5 ignores `[project]` entirely. The
conversion is to *legacy* Poetry layout, modelled on any current connector
(`source-faker/pyproject.toml`): `poetry-core` build backend, `[tool.poetry]` with
name/version/license/packages, `[tool.poetry.dependencies]`,
`[tool.poetry.scripts]`, `[tool.poetry.group.dev.dependencies]`.

**What is lost:** `uv.lock` resolution speed for local development, PEP 735
dependency groups, PEP 639 `license-files`, and the uv-based layering in our
Dockerfile — none of which the registry path can use anyway. uv survives at the
repo level as a *tool* installer, which is exactly how Airbyte's own CI uses it
(`uv tool install airbyte-cdk[dev]`).

### 2. `[tool.poe]` include block (invisible to QA, breaks everything)

Every step of Airbyte's connector CI is `poe <task>`, run from the connector
directory, and the task definitions come from the connector's own pyproject:

```toml
[tool.poe]
include = ["${POE_GIT_DIR}/poe-tasks/poetry-connector-tasks.toml"]
```

Without it, `poe install`, `poe test-unit-tests`, `poe format-check` and
`poe get-version` all fail at step one. No QA check looks for this, because the
QA checks never invoke poe. *(Found in review; verified against
`poe-tasks/poetry-connector-tasks.toml` and the CI workflow.)*

### 3. The image: our Dockerfile cannot be used at all

`airbyte_cdk.utils.docker.build_connector_image` **writes the Dockerfile itself**.
It fetches `docker-images/Dockerfile.python-connector` from the Airbyte repo and
overwrites `<connector>/build/docker/Dockerfile`. A connector's own Dockerfile is
never used, and `CheckConnectorUsesPythonBaseImage` fails if one exists at the
connector root.

Three properties of that template decide everything:

1. The builder stage runs `poetry install`, installing into the system interpreter
   under `/usr/local`.
2. The final stage is `FROM ${BASE_IMAGE}` and copies **only** `/usr/local` and
   `/airbyte/integration_code` from the builder. Our extensions live in
   `/airbyte/duckdb_extensions` — they would be silently discarded.
3. The final stage sets no `ENV` beyond `AIRBYTE_ENTRYPOINT`, and calls the console
   script named after the connector. `ERPL_EXTENSION_DIR` and `LD_LIBRARY_PATH`
   cannot be baked in, and `LD_LIBRARY_PATH` cannot be set from Python after the
   process starts.

The `.dockerignore` is an **allowlist** (`*`, then `!pyproject.toml`,
`!poetry.lock`, `!metadata.yaml`, `!build_customization.py`, `!source_*`, …), so
`bin/fetch-extensions.sh`, `bin/checksums.txt`, `uv.lock`, `LICENSE` and `main.py`
are not even in the build context.

The sanctioned hook is currently broken upstream: the CDK passes build arg
`EXTRA_BUILD_SCRIPT` (from `build_customization.py`) while the template declares
and uses `EXTRA_PREREQS_SCRIPT`. An unknown build arg is a no-op. Worth filing
upstream as a two-line fix; do not depend on it.

#### Paths for shipping 194 MB of native artefacts

Measured: `erpl_web` 45 MB, `erpl_bics` 41 MB, `erpl_rfc` 40 MB, `erpl_odp` 20 MB,
ICU + SAP NW RFC shared objects 51 MB.

| Path | Verdict |
|---|---|
| **(e) sidecar wheels** — publish `erpl-extensions-*` wheels, declare them as ordinary Poetry dependencies | **Preferred.** `poetry install` puts them in `/usr/local` site-packages, which the final stage copies. No template customisation, no policy exception, immune to base-image bumps. Largest file 45 MB, under PyPI's 100 MB per-file cap. *(Raised in review; the stage-copy mechanics are verified.)* |
| **(b) custom base image** — `datazoo/erpl-python-connector-base` pinned by digest | Workable fallback. Needs a policy exception for a third-party base image, and Airbyte's automated base-image bumps would silently drop the extensions. |
| (a) extra build script | Dead: the hook is unwired *and* `bin/` is outside the build context. |
| (c) binaries inside the connector package | Dead as framed — but (e) is this idea done properly, in separate packages. |
| (d) download at first run | Dead: breaks the no-network property and the "a sync makes no request to get.erpl.io" promise. |

Either way: delete `source-sap/Dockerfile`, stop tracking the generated
`source-sap/build/`, and move local builds onto `airbyte-cdk image build`.

#### Multi-arch is an unexamined blocker

Airbyte's publish workflow builds `linux/amd64,linux/arm64`. ERPL publishes no
arm64 build. This needs either an explicit exemption from maintainers or arm64
artefacts from ERPL. Whichever path is chosen, arm64 must fail **loudly** at
install time rather than produce an image that starts and cannot load anything.

#### Extension loading must become code-resident

`session.py:31` resolves `DEFAULT_EXTENSION_DIR` from the environment at import
time, and the runtime needs `LD_LIBRARY_PATH` for the SAP and ICU shared objects.
Neither survives the generated image. The fix is a `default_extension_dir()`
resolved inside `ErplSession.__init__` (env override first, then a path next to
the package) plus an RTLD_GLOBAL preload or `patchelf --set-rpath '$ORIGIN'`, so
the loader needs no environment. No caller depends on the current fallback:
`e2e/conftest.py` skips when the variable is unset, and every script exports it.

### 4. Metadata and hygiene

- `0.1.0` across `metadata.yaml`, the Poetry version, `CHANGELOG.md` and the
  changelog row in `sap.md` (the changelog check greps for the `dockerImageTag`)
- `dockerRepository: airbyte/source-sap`
- a square `source-sap/icon.svg` — the `icon: sap.svg` metadata key is *not* what
  the check reads, and no `.svg` exists in this repo
- `remoteRegistries.pypi.enabled` — the check only requires the key to be
  *declared*, so `false` passes if we do not want to publish
- two real HTTPS offenders: `CHANGELOG.md` (`http://get.erpl.io`) and
  `odp_odata.py:73,144`, where the literal exists precisely to detect insecure
  URLs and takes an `# ignore-https-check` comment

### 5. What Airbyte's CI should run for us

Their CI fetches connector secrets from *their* GSM. This connector's integration
and e2e tests need a live SAP system that Airbyte does not have and cannot be
given. Declaring `integrationTests` therefore makes a SAP-requiring suite blocking
on infrastructure that does not exist.

Proposal: drop `- suite: integrationTests`, bypass every credentialed category in
`acceptance-test-config.yml` (keeping `spec`), and mark credentialed tests so a
default `pytest` run skips them. Airbyte CI then blocks on unit tests (530),
format-check, image build, and image test reduced to spec plus the invalid-config
check. The 57 e2e tests keep running here, against the real system, and the PR
body links that CI.

A spec-only image test cannot tell a working image from one with no extensions in
it, so add one image test that runs `check` with an invalid config and asserts the
failure is a *connection* error rather than "Failed to initialise the ERPL
extensions".

### 6. Documentation

`docs/integrations/sources/sap.md` passes "must have user facing documentation",
and the ten structural checks skip at sl=100. Two real problems remain:

- **It is not self-contained.** Line 136 links `../../performance.md`, which exists
  here and not in `airbytehq/airbyte` — Docusaurus enforces broken links at PR
  time regardless of the QA skips. More broadly, nine sibling pages
  (authorizations, function-modules, incremental, operations, reference,
  troubleshooting, bw-queries, tables, glossary) do not ship with the connector,
  so everything a marketplace user needs must be on the one page or behind
  absolute `https://erpl.io/docs/...` links.
- **Against Airbyte's template** (`# name`, `## Prerequisites`, `## Setup guide`,
  `### For Airbyte Cloud:` / `### For Airbyte Open Source:`, `## Supported sync
  modes`, `## Supported Streams`, `## Data type map`, `## Limitations &
  Troubleshooting`, `## Changelog`), we are missing the Cloud/Open-Source split, a
  data-type map, and a troubleshooting section; Prerequisites is prose rather than
  a field-by-field mapping to the spec's UI titles.

Related: `spec.yaml` does not encode the logon-mode conditionality (direct vs
message-server vs SNC), so the documentation is the only place that knowledge
exists. Encoding it as a `oneOf` with a discriminator — as the protocol block
already does — would let the UI teach it instead.

### 7. Supply chain

`bin/checksums.txt` pins `erpl_web.duckdb_extension.gz` at `fd3c12d9…`;
`get.erpl.io` now serves `85c0b733…` for the same `v1.5.5` path. The artefact was
re-published under an existing version tag, and the pin caught it — this is why
the image job in this repo's CI has been failing. Re-pin to unblock, and fix the
policy at source: version tags that can change content are a release-integrity
problem. Both (e) and (b) remove this fetch from Airbyte's critical path.

## Questions only Airbyte can answer

These take calendar time, so they should be asked before the work lands:

1. Will a digest-pinned third-party `connectorBuildOptions.baseImage` be accepted,
   and excluded from automated base-image bumps? (Only needed if (e) is ruled out.)
2. Will an amd64-only publish be exempted from the arm64 manifest build?
3. Does community-maintained live-SAP testing in an external CI satisfy review?

And for the ERPL side: may the extensions be redistributed as PyPI wheels? That
single answer decides between (e) and (b).

## The licence problem (out of scope for this pass, unchanged)

This connector is licensed **BUSL-1.1**, matching the ERPL extensions it embeds.
`airbyte_ops_mcp/connector_qa/checks/packaging.py` fails anything outside
`{MIT, ELV2, AIRBYTE ENTERPRISE}`, with no support-level or language exemption.
The metadata *schema* accepts the string; it is the QA gate that refuses it. That
decision is tracked separately from this readiness assessment.
