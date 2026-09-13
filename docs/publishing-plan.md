# Unblocking the release chain

One artefact blocks two products. This is the plan to publish it, written down
rather than executed — nothing here has been done yet.

## The chain

```
erpl-extensions is not on PyPI
├── erpl-airbyte:  source-sap/Dockerfile cannot be deleted
│                  → "Python connectors must not use a Dockerfile" stands
│                  → the last technical QA failure before the licence question
└── erpl-dlt:      pip install "erpl-dlt[extensions]" cannot resolve
                   → the package is not installable by anyone
```

One publish clears both. Nothing else in either repository waits on anything.

## Decision: `erpl-extensions` gets its own repository

`DataZooDE/erpl-extensions`, matching how `erpl-rev` and `erpl-adt` are
structured. Two reasons:

- Its release cadence follows **ERPL** releases, not the Airbyte connector's.
  The payload *is* the version — `2026.9.4` carries ERPL `v2026.09.04` — and a
  connector bump has nothing to do with it.
- A dlt user's dependency should not be versioned by a repository named after
  Airbyte, nor share a tag namespace with a connector.

It currently lives at `erpl-airbyte/packages/erpl-extensions` (commit
`5dfea8e`). Move it with its history: `git filter-repo --path
packages/erpl-extensions`, or a fresh repo plus a single commit if the history
is not worth preserving.

## Phase 1 — Make it publishable

1. **Create `DataZooDE/erpl-extensions`** and move the package in. Note that
   repository creation returned HTTP 500 five times in a row before succeeding
   on the sixth when `erpl-dlt` was created; if it fails, it is GitHub, not the
   request.
2. **`bin/build-wheel.sh` stays as it is.** It fetches the payload through the
   connector's own `fetch-extensions.sh`, so every artefact is still verified
   against `bin/checksums.txt`, and it refuses to build if fewer than four
   extensions land or if the declared ERPL version and the payload disagree.
   The only change needed is the path to that fetcher, which today points at
   `../../source-sap/bin/fetch-extensions.sh`. Vendor it or fetch it by tag.
3. **Add `.github/workflows/release.yml`**, modelled directly on
   `erpl-rev/.github/workflows/release.yml`:
   - a build job that runs `bin/build-wheel.sh` and uploads `dist/*.whl` as an
     artifact;
   - a `pypi-publish` job gated on `startsWith(github.ref, 'refs/tags/v')`, with
     `environment: {name: pypi, url: https://pypi.org/p/erpl-extensions}`,
     `permissions: {id-token: write}` and `pypa/gh-action-pypi-publish`. No API
     token to store or rotate.
   - a `workflow_dispatch` path that builds **without** publishing, which is how
     `erpl-rev` rehearses the wheels before a tag exists. Use it first.

**Gate:** a dispatch run produces
`erpl_extensions-<version>-py3-none-manylinux_2_17_x86_64.whl`, roughly 72 MB,
and `pip install` of that artifact followed by `python -c "import
erpl_extensions; print(erpl_extensions.is_populated())"` prints `True`.

## Phase 2 — Register the pending publishers (only you can do this)

PyPI has no `register` command any more; a name is claimed by the first upload.
But Trusted Publishing has nothing to attach a publisher to before the project
exists, so each new name needs a **pending publisher** first, at
<https://pypi.org/manage/account/publishing/>:

| Field | `erpl-extensions` | `erpl-dlt` |
|---|---|---|
| PyPI project name | `erpl-extensions` | `erpl-dlt` |
| Owner | `DataZooDE` | `DataZooDE` |
| Repository | `erpl-extensions` | `erpl-dlt` |
| Workflow | `release.yml` | `release.yml` |
| Environment | `pypi` | `pypi` |

Both names are free (verified: PyPI returns 404 for each). After the first
successful upload, a pending publisher becomes an ordinary one and is never
touched again.

Then push `v2026.9.4`. **This is the irreversible step** — a PyPI version can
never be reused, so a broken wheel means a new version number, not a re-upload.

The alternative, if Trusted Publishing proves awkward: one manual
`twine upload dist/*.whl` with an API token claims the name, after which
Trusted Publishing works normally.

## Phase 3 — Unblock `erpl-airbyte`

Once the wheel is on PyPI:

1. Add `erpl-extensions = "<version>"` to `[tool.poetry.dependencies]` and
   `poetry lock` with **Poetry 1.8.5** — the version Airbyte's CI pins.
2. Delete `source-sap/Dockerfile`.
3. Point `bin/build-image.sh` and `.github/workflows/ci.yml` at
   `airbyte-cdk image build` instead of `docker build`.
4. Re-run the QA checks from a clean checkout, the way
   [registry-submission.md](registry-submission.md) describes.

**Gate:** QA reports **one** failure — the licence — down from nine failures and
one error. Plus `airbyte-cdk image test`, and
`integration_tests/test_image_carries_its_extensions.py`, which fails when the
artefacts do not survive the build. A green image test proves the sidecar wheel
reached the image, which is the whole point of the exercise.

## Phase 4 — Unblock `erpl-dlt`

1. Move `erpl-extensions` from `[project.optional-dependencies].extensions` to a
   hard runtime dependency.
2. Confirm `pip install erpl-dlt` in a clean virtualenv, from **real PyPI**
   rather than a local wheel, and that `source-sap`-free `check` against a SAP
   system still works with a bare environment (`env -i`).

**Gate:** the 142 tests still pass, 19 of them against a live system.

## Explicitly not in this plan

- **The BUSL-1.1 licence.** After Phase 3 it is the only remaining registry
  blocker, and it is a business decision rather than an engineering one.
- **The self-hosted `sap-sandbox` runner**, which has never claimed a job. It
  does not block publishing, but it does mean the e2e evidence the Airbyte PR
  would cite is not actually running in CI.
- **ODP crash recovery in `erpl-dlt`.** `recover=true` is reachable now, but no
  experiment has crashed a real extraction mid-stream to see whether SAP
  re-streams the unconfirmed packet. That blocks calling ODP production-ready,
  not publishing. See `NOTES.md` in that repository.
