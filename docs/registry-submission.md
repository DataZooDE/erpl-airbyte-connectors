# Submitting `source-sap` to the Airbyte OSS registry

## The licence problem, stated plainly

This connector is licensed **BUSL-1.1**, the same licence as the ERPL extensions
it embeds. Airbyte's connector QA gate does not accept that.

`airbyte_ops_mcp/connector_qa/consts.py`:

```python
VALID_LICENSES = {"MIT", "ELV2", "AIRBYTE ENTERPRISE"}
```

and `connector_qa/checks/packaging.py`:

```python
class CheckConnectorLicense(PackagingCheck):
    name = "Connectors must be licensed under MIT or Elv2"
```

The check carries no `applies_to_connector_support_levels` or
`applies_to_connector_languages` filter, so it applies to every connector at every
support level, `community` included. The published checklist agrees: *"Applies to
connector with any support level."*

Note the *schema* is not the obstacle. `ConnectorMetadataDefinitionV0.yaml` declares
`license: {type: string}` with no enum, so `license: BUSL-1.1` validates fine. It is
the QA check that refuses it, and the QA check runs in the submission CI.

**So a submission as-is will fail CI on exactly one check.** That is expected, and
asking for an exception is the point of submitting.

## Why BUSL rather than relaxing to MIT

Relicensing the connector glue to MIT would get it through the gate but would not
change the substance: the published image *embeds* the ERPL extensions, which are
BUSL-1.1 with an Additional Use Grant that reads:

> You may make production use of the Licensed Work. However, such use is restricted
> to the extent that it cannot be offered to third parties on a hosted or embedded
> basis.

Airbyte Cloud hosting this connector for its customers is difficult to read as
anything other than offering the work to third parties on a hosted basis. An MIT
wrapper around BUSL binaries would put a permissive label on a distribution the
underlying licence does not permit — worse than being straightforwardly refused.

That is why `registryOverrides.cloud.enabled` is `false` in `metadata.yaml`: the
OSS registry, where operators run the connector on their own infrastructure, is
compatible with the Additional Use Grant in a way Cloud is not.

## What to ask for

A licence exception for an OSS-registry, `supportLevel: community`, Cloud-disabled
connector, on these grounds:

1. It is source-available, not proprietary: the connector source is public and the
   licence converts to MPL 2.0 five years after publication.
2. It is Cloud-disabled, so Airbyte itself never hosts it.
3. The restriction protects the embedded ERPL extensions, which are a separate
   commercial product; the connector code carries no additional restriction of its
   own beyond inheriting theirs.
4. Precedent exists in spirit: Airbyte already accepts ELv2, which is likewise not
   an OSI-approved licence and likewise restricts hosted third-party offerings.

## If the exception is refused

The fallback needs no code change. The connector ships as a **self-hosted custom
connector**:

- publish the image to a registry the operator can pull from;
- the operator adds it under Settings → Sources → *Add a new connector* with the
  image name and tag;
- `metadata.yaml` stays valid and useful for anyone running `airbyte-cdk` tooling
  locally.

Everything else in this repository — the spec, the standard test suite, the docs
page, the QA-relevant metadata — is already what a registry connector needs, so the
decision can be revisited without rework.

## The rest of the checklist

Verified locally against the current QA checks:

| Check | Status |
|---|---|
| `metadata.yaml` is valid against the schema | pass |
| `connectorBuildOptions.baseImage` present and pinned by digest | pass |
| `definitionId` is a unique UUID, not copied | pass (`54427944-6368-408d-a074-cf6d556bda0f`) |
| Documentation exists at the prescribed path with the prescribed headings | pass (`docs/integrations/sources/sap.md`) |
| Every required spec field documented under Prerequisites | pass |
| Changelog version matches `dockerImageTag` | pass (1.0.0) |
| `license` is MIT or ELv2 | **fail, deliberately** |
