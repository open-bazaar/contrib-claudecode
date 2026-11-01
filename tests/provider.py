"""Claude Code specifics for the shared Harness acceptance suite."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import psutil
from bazaar_core import ContribEntry

from bazaar_contrib_claude_code import harness

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
STEERING_INPUT = "/cost"
HARNESS_ENTRY = ContribEntry(name="harness", definition=harness)
EXIT_CLOSES_SOURCE = True


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
    return ("boundary_application.py",) if scenario == "bcc" else ()


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
    processes = []
    for child in parent.children():
        try:
            if (
                any(
                    value.startswith(("--session-id=", "--resume="))
                    for value in child.cmdline()
                )
                and child.environ().get(HOME) == environment[HOME]
            ):
                processes.append(child)
        except psutil.NoSuchProcess:
            continue
    return processes


def steering_input(direction: str, packet: dict[str, Any]) -> str | None:
    return packet["uuid"] if direction == "sent" and packet["type"] == "user" else None


def steering_settled(direction: str, packet: dict[str, Any]) -> str | None:
    if packet["type"] == "command_lifecycle" and packet["state"] in {
        "completed",
        "cancelled",
        "discarded",
        "refused",
    }:
        return packet["command_uuid"]
    return None


def steering_result(direction: str, packet: dict[str, Any]) -> dict[str, Any] | None:
    if packet["type"] != "result":
        return None
    return {
        key: packet.get(key)
        for key in ("user_message_uuids", "queued_turn_count", "result_index")
    }
