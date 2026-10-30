"""Prepare the real local workspace application without touching the host node."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tomllib
from importlib.metadata import distributions as installed_distributions
from importlib.metadata import version
from pathlib import Path
from uuid import uuid4

import tomli_w

root = Path(sys.argv[1]).resolve()
root.mkdir(exist_ok=True)
python = Path(sys.executable)
model = sys.argv[2]
distributions = Path(sys.argv[3]).resolve()
agent_root = Path(sys.argv[4]).resolve()
scenario = sys.argv[5]
environment = {
    name: value
    for name, value in os.environ.items()
    if not name.startswith(
        (
            "BCN_",
            "BCC_",
            "CODEX_",
            "CLAUDE_",
            "ANTHROPIC_",
            "OPENAI_",
            "MYSCRM_",
            "APOLLO_",
        )
    )
}
endpoint = (
    rf"\\.\pipe\bccj-{uuid4()}"
    if os.name == "nt"
    else "/tmp/bccj-" + str(uuid4()) + ".sock"
)
environment.update(
    ANTHROPIC_BASE_URL=environment["AI_GATEWAY_BASE_URL"],
    ANTHROPIC_API_KEY=environment["AI_API_KEY"],
    BCN_HOME=str(root / "home"),
    CLAUDE_CONFIG_DIR=str(agent_root / "claude-config"),
    PATH=str(python.parent) + os.pathsep + os.environ["PATH"],
)
if scenario == "mcp-once":
    environment["ANTHROPIC_API_KEY"] = "inherited-key-overridden-by-config"
(root / "environment.private.json").write_text(
    json.dumps(
        {
            name: environment[name]
            for name in (
                "BCN_HOME",
                "CLAUDE_CONFIG_DIR",
                "PATH",
                "ANTHROPIC_BASE_URL",
                "ANTHROPIC_API_KEY",
            )
        }
    )
)
(root / "environment.private.json").chmod(0o600)
home = Path(environment["CLAUDE_CONFIG_DIR"])
home.mkdir(parents=True, exist_ok=True)
workspace = agent_root / "workspace"
workspace.mkdir(parents=True, exist_ok=True)
agents = [
    {
        "id": "assistant",
        "name": "Assistant",
        "workspace": str(workspace),
        "mode": "session",
        "channel": [{"kind": "test-channel", "audit_file": str(root / "audit.jsonl")}],
        "harness": [{"kind": "claudecode", "model": model}],
    }
]
if scenario == "mcp-once":
    agents[0]["harness"][0]["api_key"] = environment["AI_API_KEY"]
# Linux's native Claude sandbox cannot expose a preview port to the host.
if scenario in {"full-access", "background"}:
    agents[0]["session"] = {"config": {"sandbox": {"policy": "danger-full-access"}}}
if scenario == "refresh":
    for name in ("AGENTS.md", "CLAUDE.md"):
        (workspace / name).write_text("Keep replies clear.\n")
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
                    "args": ["-m", "bcn_claude_test_channel.mcp"],
                }
            },
        }
    )
)
config = root / "node.toml"
command = [str(python), "-m", "bazaar_compute_node"]
print("Bootstrap installed application", config, flush=True)
result = subprocess.run(
    [*command, "--configer-options", str(config), "--help"],
    env=environment,
    check=False,
    capture_output=True,
    text=True,
)
(root / "bootstrap.log").write_text(result.stdout + result.stderr)
print(result.stdout + result.stderr, end="", flush=True)
if result.returncode:
    raise RuntimeError("Application bootstrap failed; see bootstrap.log")
document = tomllib.loads(config.read_text())
for package in document["contribs"]:
    for entry in package["enabled"]:
        if entry["entry"] == "agent":
            entry["config"]["agents"] = agents
        elif entry["entry"] in {"sidecar-unix", "sidecar-windows"}:
            entry["config"] = {"endpoint": endpoint}
document["contribs"] = [
    package
    for package in document["contribs"]
    if package["package"]
    not in {"bcn-claude-test-channel", "bazaar-contrib-claude-code"}
]
for package, entry in (
    ("bcn-claude-test-channel", "test-channel-builder"),
    ("bazaar-contrib-claude-code", "claudecode-builder"),
):
    (wheel,) = distributions.glob(package.replace("-", "_") + "-*.whl")
    document["contribs"].append(
        {
            "package": package + " @ " + wheel.as_uri(),
            "enabled": [{"entry": entry}],
        }
    )
config.write_text(tomli_w.dumps(document))
print("Activate provider and test channel", config, flush=True)
with (root / "activation.log").open("w") as log:
    result = subprocess.run(
        [*command, "--configer-options", str(config), "--help"],
        env=environment,
        check=False,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
print((root / "activation.log").read_text(), end="", flush=True)
result.check_returncode()
os.environ.update(environment)
print(
    json.dumps(
        {
            "prepared": str(root),
            "model": model,
            "endpoint": endpoint,
            "bcn_version": version("bazaar-compute-node"),
            "agent_id": "assistant",
            "workspace": str(workspace),
            "claude_config_dir": str(home),
            "bcn_packages": {
                package.metadata["Name"]: package.version
                for package in installed_distributions()
                if package.metadata["Name"].startswith("bazaar-")
            },
        }
    ),
    flush=True,
)
