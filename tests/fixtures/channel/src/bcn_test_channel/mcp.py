"""A minimal MCP tool for real invocation and approval checks."""

from __future__ import annotations

from mcp.server import MCPServer

server = MCPServer("test")


@server.tool()
async def get_value(key: str) -> dict[str, str]:
    """Return the fixed value MCP_OK for a test key."""
    return {"key": key, "value": "MCP_OK"}


if __name__ == "__main__":
    server.run()
