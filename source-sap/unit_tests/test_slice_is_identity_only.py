"""`ReadPlan.slice_` is the partition's identity; `ReadPlan.meta` is machinery.

The distinction is not decorative: `slice_` is what the CDK's slice logger
prints and what a partition compares equal by, so anything in it is read by a
human debugging a sync and must mean something to one. BICS put its DuckDB
session id there -- a derived handle nobody outside the driver can act on --
which is exactly the drift this walks every driver to catch.
"""

from __future__ import annotations

import pytest

from source_sap.protocols.base import SapObject
from source_sap.source import DRIVERS

CONN = {"ashost": "h", "sysnr": "00", "client": "001", "user": "u", "password": "p"}

#: Keys that belong in `meta`. Names, not shapes, because that is how they drift
#: back in: someone needs a handle at read time and reaches for the nearest dict.
MACHINERY = {"setup", "session_id", "statements", "sql", "params", "cursor", "connection"}

CASES = {
    "rfc": ({"objects": [{"name": "T"}]}, SapObject(name="T", json_schema={}, meta={"table": "T"})),
    "bics": (
        {"objects": [{"name": "Q", "cube": "C", "query": "Q"}]},
        SapObject(name="Q", json_schema={}, meta={"cube": "C", "query": "Q", "session_id": "s"}),
    ),
    "bics_sliced": (
        {
            "objects": [
                {
                    "name": "Q",
                    "cube": "C",
                    "query": "Q",
                    "slice_by": {"characteristic": "0CALMONTH", "members": ["202601", "202602"]},
                }
            ]
        },
        SapObject(name="Q", json_schema={}, meta={"cube": "C", "query": "Q", "session_id": "s"}),
    ),
    "odp_odata": (
        {"objects": [{"entity_set": "E", "url": "http://gw/x"}]},
        SapObject(name="E", json_schema={}, meta={"entity_set": "E", "entity_set_url": "http://gw/x"}),
    ),
}


def _plans(mode, protocol, target):
    driver = DRIVERS[mode]({**CONN, "protocol": {"mode": mode, **protocol}})
    return driver.read_plans(None, target, incremental=False, state={})


@pytest.mark.parametrize("case", sorted(CASES))
class TestPartitionIdentityIsReadable:
    def _plans(self, case):
        protocol, target = CASES[case]
        return _plans(case.split("_sliced")[0], protocol, target)

    def test_no_machinery_key_reaches_the_slice(self, case):
        for plan in self._plans(case):
            leaked = sorted(set(plan.slice_) & MACHINERY)
            assert not leaked, f"{case}: ReadPlan.slice_ carries machinery: {', '.join(leaked)}"

    def test_every_slice_value_is_a_scalar(self, case):
        # A nested structure in the identity means machinery came with it.
        for plan in self._plans(case):
            for key, value in plan.slice_.items():
                assert isinstance(value, (str, int, float, bool, type(None))), f"{case}: {key} is {type(value)}"

    def test_the_slice_identifies_the_partition(self, case):
        plans = self._plans(case)
        identities = [tuple(sorted(p.slice_.items())) for p in plans]
        assert len(set(identities)) == len(plans), f"{case}: two partitions share one identity"
        assert all(p.slice_ for p in plans), f"{case}: a partition has no identity to log"
