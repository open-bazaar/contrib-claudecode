"""Run acceptance with one prefix preheat on the owned setup-ai-gateway backend."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

from gateway_resources import discover, docker, inspect


def main() -> int:
    if sys.platform != "linux":
        raise RuntimeError("Gateway acceptance requires the Linux Docker runner")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--cache-dir", type=Path, default=Path(".artifacts/prefix-cache")
    )
    parser.add_argument("--upstream", default="http://model:8080")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    arguments = parser.parse_args()
    command = arguments.command
    if command[:1] == ["--"]:
        command = command[1:]
    if not command:
        parser.error("Supply the original acceptance command after --")
    output = arguments.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    cache = arguments.cache_dir.resolve()
    cache.mkdir(parents=True, exist_ok=True)
    identifiers = discover()
    model = next(
        inspect(identifier)
        for identifier in identifiers
        if inspect(identifier)["labels"]["com.docker.compose.service"] == "model"
    )
    labels = model["labels"]
    project = labels["com.docker.compose.project"]
    compose = [
        "docker",
        "compose",
        "--project-name",
        project,
    ]
    for configuration in labels["com.docker.compose.project.config_files"].split(","):
        compose.extend(["--file", configuration])
    name = project + "-preheat-" + uuid4().hex[:8]
    state: dict = {"project": project, "container_name": name, "model_id": model["id"]}
    routed = False
    process = None
    returncode = 1

    gateway = next(Path(os.environ["RUNNER_TEMP"]).glob("ai-gateway-*/state.json"))
    environment = os.environ | json.loads(gateway.read_text())["environment"]

    def run(*items: str):
        subprocess.run(items, check=True, env=environment)

    def provider(url: str):
        # Keep the action's existing local/local-model entry throughout the suite.
        run(
            *compose,
            "exec",
            "-T",
            "magpie",
            "magpie",
            "provider",
            "set",
            "local",
            "url=" + url.rstrip("/") + "/v1",
            "key=local-only",
            "models=local-model",
        )

    try:
        configuration = json.loads(
            docker("inspect", "--format", "{{json .Config}}", model["id"])
        )
        metadata = next(Path(os.environ["RUNNER_TEMP"]).glob("ai-gateway-*/model.json"))
        (output / "cache-identity.json").write_text(
            json.dumps(
                {
                    "model_sha256": json.loads(metadata.read_text())["sha256"],
                    "image": configuration["Image"],
                    "command": configuration["Cmd"],
                }
            )
        )
        directory = output / "cache-model"
        directory.mkdir(exist_ok=True)
        configuration = directory / "compose.yml"
        configuration.write_text(
            json.dumps(
                {
                    "services": {
                        "model": {
                            "environment": {"LLAMA_ARG_SLOT_SAVE_PATH": "/cache"},
                            "volumes": [f"{cache}:/cache"],
                        }
                    }
                }
            )
        )
        compose.extend(["--file", str(configuration)])
        run(
            *compose,
            "up",
            "--detach",
            "--force-recreate",
            "--no-deps",
            "--wait",
            "model",
        )
        state["model_id"] = subprocess.run(
            (*compose, "ps", "--quiet", "model"),
            capture_output=True,
            text=True,
            check=True,
            env=environment,
        ).stdout.strip()
        run(
            "docker",
            "run",
            "--detach",
            "--name",
            name,
            "--label",
            "bcn.acceptance-preheat=" + project,
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "--network",
            project + "_default",
            "--network-alias",
            name,
            "--memory",
            "256m",
            "--cpus",
            "1",
            "--mount",
            "type=bind,src="
            + str(Path(__file__).with_name("gateway_preheat.mjs").resolve())
            + ",dst=/preheat.mjs,readonly",
            "--mount",
            f"type=bind,src={output},dst=/evidence",
            "--mount",
            f"type=bind,src={cache},dst=/cache",
            "node:24.18.0-bookworm-slim",
            "node",
            "/preheat.mjs",
            arguments.upstream,
        )
        state["container"] = inspect(name)
        for attempt in range(100):
            if (output / "preheat.jsonl").exists():
                break
            if not inspect(name)["state"]["Running"]:
                raise RuntimeError("Owned preheat proxy exited before becoming ready")
            if attempt == 99:
                raise RuntimeError("Owned preheat proxy did not become ready")
            time.sleep(0.1)
        provider("http://" + name + ":8080")
        routed = True
        print("Acceptance prefix preheat ready on", project, flush=True)
        process = subprocess.Popen(command)
        returncode = process.wait()
    except BaseException as error:
        state["failure"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            process.wait()
        try:
            if routed:
                provider(arguments.upstream)
        finally:
            owned = []
            try:
                owned = docker(
                    "ps",
                    "--all",
                    "--quiet",
                    "--filter",
                    "name=^/" + name + "$",
                    "--filter",
                    "label=bcn.acceptance-preheat=" + project,
                ).split()
                state["proxy_created"] = bool(owned)
                if owned:
                    state["container_before_stop"] = inspect(name)
                    run("docker", "stop", "--time", "10", name)
                    state["container_after_stop"] = inspect(name)
                    (output / "proxy.log").write_text(docker("logs", name))
            finally:
                if owned:
                    subprocess.run(["docker", "rm", "--force", name], check=False)
                state["remaining_owned_proxy"] = docker(
                    "ps", "--all", "--quiet", "--filter", "name=^/" + name + "$"
                ).split()
                state["test_returncode"] = returncode
                (output / "lifecycle.json").write_text(
                    json.dumps(state, indent=2) + "\n"
                )
    return returncode if returncode >= 0 else 128 - returncode


if __name__ == "__main__":
    sys.exit(main())
