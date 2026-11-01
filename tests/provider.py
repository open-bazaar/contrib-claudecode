"""Claude Code specifics for the shared Harness acceptance suite."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import psutil

KIND = "claudecode"
PACKAGE = "bazaar-contrib-claude-code"
BUILDER = "claudecode-builder"
HOME = "CLAUDE_CONFIG_DIR"
HOME_DIRECTORY = "claude-config"
API_KEY = "ANTHROPIC_API_KEY"
PRIVATE = ("ANTHROPIC_BASE_URL", "ANTHROPIC_API_KEY")
SCENARIOS = ("mcp-once", "bcc", "full-access", "recovery", "refresh", "background")
# Linux's native Claude sandbox cannot expose a preview port to the host.
FULL_ACCESS = frozenset({"full-access", "background"})
INSTRUCTION_FILES = ("AGENTS.md", "CLAUDE.md")
MCP_TOOL = "mcp__test__get_value"
MCP_APPROVAL = "mcp__test__get_value"
COMMAND_APPROVAL = "Bash"
BACKGROUND = (
    "with the Bash tool's `run_in_background` option, without a trailing `&`. "
    "Keep that background task running after this turn."
)


def configure(
    environment: dict[str, str],
    home: Path,
    workspace: Path,
    python: Path,
    model: str,
    scenario: str,
) -> None:
    environment.update(
        ANTHROPIC_BASE_URL=environment["AI_GATEWAY_BASE_URL"],
        ANTHROPIC_API_KEY=environment["AI_API_KEY"],
    )
    (home / "settings.json").write_text(
        json.dumps(
            {
                "enableAllProjectMcpServers": True,
                "env": {
                    "ENABLE_TOOL_SEARCH": "false",
                    "MCP_CONNECTION_NONBLOCKING": "0",
                },
                "permissions": {"ask": ["Bash(bcc *)"]},
            }
        )
    )
    (workspace / ".mcp.json").write_text(
        json.dumps(
            {
                "mcpServers": {
                    "test": {
                        "command": str(python),
                        "args": ["-m", "bcn_test_channel.mcp"],
                    }
                },
            }
        )
    )


def applications(scenario: str) -> tuple[str, ...]:
    if scenario == "bcc":
        return ("lifecycle_application.py", "steering_application.py")
    return ("lifecycle_application.py",)


def bcc_command(call: dict[str, Any]) -> str | None:
    command = call["input"].get("command") if isinstance(call["input"], dict) else None
    return command if isinstance(command, str) else None


def mcp_output(call: dict[str, Any]) -> Any:
    return json.loads(call["output"])


def approval_call_id(approval: dict[str, Any]) -> str:
    return approval["request_id"]


def session_processes(
    parent: psutil.Process, environment: dict[str, str]
) -> list[psutil.Process]:
    return [
        child
        for child in parent.children()
        if any(
            value.startswith(("--session-id=", "--resume="))
            for value in child.cmdline()
        )
        and child.environ().get(HOME) == environment[HOME]
    ]
