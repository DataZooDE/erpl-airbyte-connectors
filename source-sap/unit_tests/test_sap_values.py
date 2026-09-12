"""SAP scalar value parsing, extracted from the invoke driver.

These are about SAP's wire formats, not about calling function modules, and two
protocols now need them.
"""

import pytest

from source_sap.sap_values import sap_date, sap_time, sap_timestamp


class TestSapDate:
    @pytest.mark.parametrize(
        "given,expected",
        [
            ("20260102", "2026-01-02"),
            ("2026-01-02", "2026-01-02"),
            (" 20260102 ", "2026-01-02"),
        ],
    )
    def test_accepted_forms(self, given, expected):
        assert sap_date(given) == expected

    @pytest.mark.parametrize("bad", ["not-a-date", "20261301", "", "2026"])
    def test_rejected_forms(self, bad):
        with pytest.raises(ValueError):
            sap_date(bad)


class TestSapTime:
    @pytest.mark.parametrize(
        "given,expected",
        [
            ("103000", "10:30:00"),
            ("10:30:00", "10:30:00"),
            ("1030", "10:30:00"),
            ("10:30", "10:30:00"),
        ],
    )
    def test_accepted_forms(self, given, expected):
        assert sap_time(given) == expected

    @pytest.mark.parametrize("bad", ["251000", "nonsense", ""])
    def test_rejected_forms(self, bad):
        with pytest.raises(ValueError):
            sap_time(bad)


class TestSapTimestamp:
    @pytest.mark.parametrize(
        "given",
        [
            "20260102103000",
            "2026-01-02T10:30:00",
            "2026-01-02 10:30:00",
        ],
    )
    def test_accepted_forms(self, given):
        assert sap_timestamp(given) == "2026-01-02 10:30:00"

    def test_rejected_form(self):
        with pytest.raises(ValueError):
            sap_timestamp("yesterday")
