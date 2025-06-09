"""
Pytest configuration file with shared fixtures.
"""
import pytest
import logging
import os

# Disable logging during tests to keep output clean
@pytest.fixture(autouse=True)
def disable_logging():
    """Disable logging during tests."""
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)

# Skip integration tests if environment variable is set
def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )

def pytest_collection_modifyitems(config, items):
    if os.environ.get("SKIP_INTEGRATION_TESTS", "False").lower() == "true":
        skip_integration = pytest.mark.skip(reason="Integration tests skipped")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)