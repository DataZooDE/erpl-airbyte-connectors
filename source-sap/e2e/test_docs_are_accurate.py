"""The documentation makes checkable claims. Check them.

Documentation that is confidently wrong is worse than documentation that is
missing, and these particular claims drift: ERPL adds function modules, the spec
gains settings, clamps get retuned.
"""

from __future__ import annotations

import pathlib
import re

import pytest
import yaml

DOCS = pathlib.Path(__file__).resolve().parents[2] / "docs"
SPEC = yaml.safe_load((pathlib.Path(__file__).resolve().parents[1] / "source_sap" / "spec.yaml").read_text())


def _rfc_branch() -> dict:
    return next(
        b
        for b in SPEC["connectionSpecification"]["properties"]["protocol"]["oneOf"]
        if b["properties"]["mode"]["const"] == "rfc"
    )


class TestAuthorizationsPageIsComplete:
    """`docs/authorizations.md` is handed to a Basis team to grant S_RFC from.

    A module the connector calls but the page omits is an authorization the team
    will not grant, and a sync that fails in production rather than here.
    """

    @pytest.mark.requires_creds
    def test_every_function_module_erpl_declares_is_documented(self, erpl_extensions):
        import duckdb

        con = duckdb.connect(config={"allow_unsigned_extensions": "true", "extension_directory": erpl_extensions})
        try:
            for extension in ("erpl_rfc", "erpl_bics", "erpl_odp"):
                con.load_extension(extension)
            declared = {
                row[0]
                for row in con.sql(
                    "SELECT DISTINCT rfc_function_module FROM sap_rfc_authorizations() "
                    "WHERE rfc_function_module NOT IN ('<user-specified>', '<none>')"
                ).fetchall()
            }
        finally:
            con.close()

        page = (DOCS / "authorizations.md").read_text()
        missing = sorted(module for module in declared if module not in page)
        assert not missing, (
            "docs/authorizations.md omits function modules the connector calls, so a "
            f"Basis team granting S_RFC from it would miss them: {', '.join(missing)}"
        )
        assert len(declared) > 20, "the declaration looks truncated"


class TestReferencePageMatchesTheSpec:
    def _limits(self) -> dict[str, str]:
        section = (DOCS / "reference.md").read_text().split("## Limits", 1)[1]
        rows = (
            line.strip("|").split("|") for line in section.splitlines() if line.startswith("|") and "---" not in line
        )
        return {r[0].strip("` "): r[1].strip() for r in rows if len(r) == 2}

    def test_every_protocol_mode_has_a_section(self):
        page = (DOCS / "reference.md").read_text()
        modes = {
            b["properties"]["mode"]["const"] for b in SPEC["connectionSpecification"]["properties"]["protocol"]["oneOf"]
        }
        missing = [m for m in modes if f"`{m}`" not in page]
        assert not missing, f"reference.md documents no section for: {missing}"

    def test_the_concurrency_range_matches(self):
        prop = SPEC["connectionSpecification"]["properties"]["concurrency"]
        assert self._limits()["concurrency"] == f"{prop['minimum']}–{prop['maximum']}"

    def test_the_partitions_range_matches(self):
        prop = _rfc_branch()["properties"]["partitions"]
        assert self._limits()["partitions"] == f"{prop['minimum']}–{prop['maximum']}"

    def test_the_documented_fetch_size_cap_matches_the_code(self):
        from source_sap.protocols.rfc import MAX_FETCH_SIZE

        assert f"{MAX_FETCH_SIZE:,}" in self._limits()["fetch_size"]

    def test_partitions_still_defaults_to_zero(self):
        # The performance document argues at length for this default.
        assert _rfc_branch()["properties"]["partitions"]["default"] == 0


class TestReferenceDocumentsEveryField:
    """`docs/reference.md` claims to list every field, and is maintained by hand.

    It had already drifted: `return_parameter` was documented but missing from
    the spec, and BICS filters, variants and display properties were supported
    by the driver but reachable from neither.
    """

    def _documented(self) -> set[str]:
        page = (DOCS / "reference.md").read_text()
        return set(re.findall(r"`([a-z_]+(?:\[\]\.[a-z_]+)?)`", page))

    def _spec_fields(self) -> set[str]:
        fields = set()
        for branch in SPEC["connectionSpecification"]["properties"]["protocol"]["oneOf"]:
            for name in branch["properties"]:
                if name != "mode":
                    fields.add(name)
            items = (branch["properties"].get("objects") or {}).get("items", {})
            for name in items.get("properties", {}):
                fields.add(f"objects[].{name}")
        for name in SPEC["connectionSpecification"]["properties"]:
            if name != "protocol":
                fields.add(name)
        return fields

    def test_every_spec_field_appears_in_the_reference(self):
        documented = self._documented()
        missing = sorted(
            field
            for field in self._spec_fields()
            if field not in documented and field.split("].")[-1] not in documented
        )
        assert not missing, "docs/reference.md says it lists every field but omits: " + ", ".join(missing)


class TestInternalLinksResolve:
    def test_no_documentation_link_is_broken(self):
        root = DOCS.parent
        broken = []
        for md in list(root.glob("*.md")) + list(root.glob("docs/**/*.md")):
            for label, target in re.findall(r"\[([^\]]+)\]\(([^)]+)\)", md.read_text()):
                if target.startswith(("http", "#", "mailto:")):
                    continue
                if not (md.parent / target.split("#")[0]).resolve().exists():
                    broken.append(f"{md.relative_to(root)}: [{label}]({target})")
        assert not broken, "broken documentation links:\n  " + "\n  ".join(broken)
