import sys


def pytest_configure(config):
    if sys.version_info >= (3, 13):
        raise SystemExit(
            "Integration tests require Python < 3.13 (grpcio wheel availability). "
            "Run with 'python3.11 -m pytest'."
        )
