"""Ask the actual CLI for its version, models, workspace skills and MCP servers."""

from __future__ import annotations

import asyncio
import shutil
from collections.abc import Mapping, Sequence
from pathlib import Path

from bazaar_contrib_harness import (
    HarnessInspection,
    HarnessMcpServer,
    HarnessMcpTool,
    HarnessModel,
    McpAuth,
    McpConnection,
    McpFailed,
)

from .children import Children
from .client import Client
from .environment import environment
from .process import ProcessSpec, ProcessSupervisor
from .protocol import ClaudeTransportError
from .runtime import _check_version
from .skills import _read_skills


async def inspect(workspace: Path, *, timeout: float) -> HarnessInspection:  # noqa: ASYNC109
    executable = await asyncio.to_thread(shutil.which, "claude")
    if executable is None:
        return HarnessInspection(available=False, error="not found")
    version: str | None = None
    children = Children()
    supervisor: ProcessSupervisor | None = None
    client: Client | None = None
    try:
        parts = await _check_version(
            executable, workspace, environment(), timeout=timeout
        )
        version = ".".join(str(part) for part in parts)
        supervisor = ProcessSupervisor(
            ProcessSpec(
                executable=executable,
                arguments=(
                    "-p",
                    "--input-format",
                    "stream-json",
                    "--output-format",
                    "stream-json",
                    "--verbose",
                ),
                cwd=workspace,
                environment=environment(),
            ),
            children,
        )
        client = Client(supervisor)
        await supervisor.start(timeout=timeout)
        initialized = await client.initialize(timeout=timeout)
        answer = await client.control({"subtype": "list_models"}, timeout=timeout)
        servers = await client.control({"subtype": "mcp_status"}, timeout=timeout)
        return HarnessInspection(
            available=True,
            version=version,
            models=_read_models(answer.get("response")),
            skills=_read_skills(initialized),
            mcp_servers=_read_mcp_servers(servers.get("response")),
        )
    except (ClaudeTransportError, OSError, TimeoutError) as error:
        return HarnessInspection(
            available=version is not None,
            version=version,
            error=str(error) or type(error).__name__,
        )
    finally:
        try:
            if client is not None:
                await client.close()
            if supervisor is not None:
                await supervisor.stop(timeout=timeout)
        finally:
            await children.end_all()


def _read_models(response: object) -> tuple[HarnessModel, ...]:
    """Each model as `list_models` names it: the value `--model` takes, the
    name to show, and the efforts it supports - none for one that does not
    take an effort."""

    if not isinstance(response, Mapping):
        return ()
    items = response.get("models")
    if not isinstance(items, Sequence):
        return ()
    models: list[HarnessModel] = []
    for item in items:
        if not isinstance(item, Mapping):
            continue
        value = item.get("value")
        if not isinstance(value, str) or not value:
            continue
        name = item.get("displayName")
        efforts = item.get("supportedEffortLevels")
        models.append(
            HarnessModel(
                id=value,
                name=name if isinstance(name, str) else value,
                efforts=tuple(effort for effort in efforts if isinstance(effort, str))
                if isinstance(efforts, Sequence) and not isinstance(efforts, str)
                else (),
                # Claude Code lists what it answers as unasked under this value
                default=value == "default",
            )
        )
    return tuple(models)


# Claude Code's connection states as the common ones
_CONNECTION = {
    "connected": McpConnection.CONNECTED,
    "pending": McpConnection.CONNECTING,
    "disabled": McpConnection.DISABLED,
}


def _read_mcp_servers(response: object) -> tuple[HarnessMcpServer, ...]:
    """Each MCP server as `mcp_status` gives it: its name, how its connection
    stands, failed with the error it gives, a sign-in it still needs, and the
    tools it offers, which Claude Code names without their schemas; it says
    nothing of a sign-in otherwise."""

    if not isinstance(response, Mapping):
        return ()
    items = response.get("mcpServers")
    if not isinstance(items, Sequence):
        return ()
    servers: list[HarnessMcpServer] = []
    for item in items:
        if not isinstance(item, Mapping):
            continue
        name = item.get("name")
        if not isinstance(name, str) or not name:
            continue
        status = item.get("status")
        error = item.get("error")
        tools = item.get("tools")
        servers.append(
            HarnessMcpServer(
                name=name,
                connection=McpFailed(
                    reason=error if isinstance(error, str) and error else "failed"
                )
                if status == "failed"
                else _CONNECTION.get(status)
                if isinstance(status, str)
                else None,
                auth=McpAuth.SIGNED_OUT if status == "needs-auth" else None,
                tools=tuple(
                    HarnessMcpTool(
                        name=tool["name"],
                        description=tool["description"]
                        if isinstance(tool.get("description"), str)
                        else None,
                        input_schema=tool["inputSchema"]
                        if isinstance(tool.get("inputSchema"), Mapping)
                        else None,
                    )
                    for tool in tools
                    if isinstance(tool, Mapping) and isinstance(tool.get("name"), str)
                )
                if isinstance(tools, Sequence)
                else (),
            )
        )
    return tuple(servers)
