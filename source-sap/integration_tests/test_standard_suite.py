"""Airbyte's standard connector tests, driven from acceptance-test-config.yml."""

import pytest

pytest_plugins = ["airbyte_cdk.test.standard_tests.pytest_hooks"]

standard_tests = pytest.importorskip("airbyte_cdk.test.standard_tests")


class TestSuiteSourceSap(standard_tests.SourceTestSuiteBase):
    """Runs spec / check / discover / basic_read against secrets/config.json."""
