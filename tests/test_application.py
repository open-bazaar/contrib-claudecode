"""Install published BCN and the provider, then observe actual bcc use."""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import pytest


@pytest.mark.e2e
@pytest.mark.parametrize(
    "scenario",
    [
        "mcp-once",
        "bcc",
        "full-access",
        "recovery",
        "refresh",
        "background",
    ],
)
async def test_installed_application(
    tmp_path: Path,
    pytestconfig: pytest.Config,
    scenario: str,
    shared_agent_root: Path,
):
    model = pytestconfig.getoption("--model")
    if (
        not model
        or not os.environ.get("AI_GATEWAY_BASE_URL")
        or not os.environ.get("AI_API_KEY")
    ):
        pytest.skip(
            "Supply --model and setup-ai-gateway's environment for real inference"
        )
    source = (await asyncio.to_thread(Path(__file__).resolve)).parents[1]
    distributions = tmp_path / "distributions"
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(
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
    } | {
        "BCN_HOME": str(tmp_path / "home"),
        "UV_TOOL_DIR": str(tmp_path / "tools"),
        "UV_TOOL_BIN_DIR": str(tmp_path / "bin"),
    }

    async def execute(label: str, *arguments: str):
        started = asyncio.get_running_loop().time()
        await asyncio.to_thread(print, "Run", label, *arguments, flush=True)
        output = await asyncio.to_thread((tmp_path / (label + ".log")).open, "wb")

        def record(chunk: bytes):
            output.write(chunk)
            output.flush()
            sys.stdout.buffer.write(chunk)
            sys.stdout.buffer.flush()

        try:
            process = await asyncio.create_subprocess_exec(
                *arguments,
                cwd=source,
                env=environment,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            assert process.stdout is not None
            while chunk := await process.stdout.read(8192):
                await asyncio.to_thread(record, chunk)
            await process.wait()
            await asyncio.to_thread(
                print,
                f"Finished {label}: exit={process.returncode}, "
                f"elapsed={asyncio.get_running_loop().time() - started:.2f}s",
                flush=True,
            )
        finally:
            await asyncio.to_thread(output.close)
        assert process.returncode == 0, await asyncio.to_thread(
            (tmp_path / (label + ".log")).read_text
        )

    await execute(
        "build-provider",
        "uv",
        "build",
        "--no-sources",
        "--wheel",
        "--out-dir",
        str(distributions),
    )
    await execute(
        "build-channel",
        "uv",
        "build",
        str(source / "tests/fixtures/channel"),
        "--no-sources",
        "--wheel",
        "--out-dir",
        str(distributions),
    )
    wheels = await asyncio.to_thread(lambda: tuple(distributions.glob("*.whl")))
    arguments = [
        "uv",
        "tool",
        "install",
        "--python",
        "3.15",
        "--prerelease",
        "allow",
        "--refresh",
        "--default-index",
        "https://pypi.org/simple",
        "bazaar-compute-node>=0.3.0rc10,<0.4",
    ]
    for wheel in wheels:
        arguments.extend(["--with", str(wheel)])
    await execute("install", *arguments)
    await execute(
        "python",
        "uv",
        "python",
        "find",
        "--no-project",
        str(tmp_path / "tools/bazaar-compute-node"),
    )
    python = (await asyncio.to_thread((tmp_path / "python.log").read_text)).strip()
    root = tmp_path / scenario
    await execute(
        "bcc-prepare",
        str(python),
        str(source / "tests/prepare_application.py"),
        str(root),
        model,
        str(distributions),
        str(shared_agent_root),
        scenario,
    )
    await execute(
        "lifecycle",
        str(python),
        str(source / "tests/lifecycle_application.py"),
        str(root),
        str(shared_agent_root / "workspace"),
    )
    if scenario == "bcc":
        await execute(
            "steering",
            str(python),
            str(source / "tests/steering_application.py"),
            str(root),
            str(shared_agent_root / "workspace"),
        )
    await execute(
        "bcc",
        str(python),
        str(source / "tests/bcc_application.py"),
        str(root),
        scenario,
        str(shared_agent_root / "workspace"),
    )
