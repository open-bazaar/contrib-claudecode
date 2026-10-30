"""Inputs supplied by the real-model acceptance environment."""

from pathlib import Path

import pytest


def pytest_addoption(parser: pytest.Parser):
    parser.addoption("--model", help="Actual model ID returned by setup-ai-gateway")


@pytest.fixture(scope="session")
def shared_agent_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Keep model-visible workspace and built-in skill paths stable across cases."""
    return tmp_path_factory.mktemp("shared-agent")
