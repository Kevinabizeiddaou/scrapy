from __future__ import annotations

from pathlib import Path

import pytest

from wrc_pipeline.config import Settings

FIXTURES = Path(__file__).parent / "fixtures"


def load_fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


@pytest.fixture
def fixture_text() -> callable:
    return load_fixture


@pytest.fixture
def test_settings() -> Settings:
    """Settings isolated from any developer ``.env`` file."""
    return Settings(
        _env_file=None,
        mongo_uri="mongodb://localhost:27017",
        mongo_database="wrc_landing_test",
        landing_bucket="wrc-landing-test",
        s3_create_bucket=False,
    )
