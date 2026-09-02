from __future__ import annotations

from pathlib import Path

FIXTURES = Path(__file__).parent / "fixtures"


def load_fixture(name: str) -> str:
    """Read a page captured from the live site, for parser and transformer tests."""
    return (FIXTURES / name).read_text(encoding="utf-8")
