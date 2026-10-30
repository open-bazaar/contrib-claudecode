"""Observe this CI gateway's actual memory while preserving pytest's exit status."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path


def docker(*arguments: str) -> str:
    result = subprocess.run(
        ["docker", *arguments], capture_output=True, text=True, timeout=15, check=False
    )
    if result.returncode:
        raise RuntimeError(f"docker {arguments[0]} exited {result.returncode}")
    return result.stdout


def inspect(identifier: str) -> dict:
    # Select fields explicitly: Config.Env and gateway credentials are never read.
    template = "\t".join(
        "{{json ." + field + "}}"
        for field in (
            "Id",
            "Config.Labels",
            "Config.Image",
            "State",
            "HostConfig.Memory",
            "HostConfig.NanoCpus",
        )
    )
    values = [
        json.loads(field)
        for field in docker("inspect", "--format", template, identifier)
        .strip()
        .split("\t")
    ]
    return dict(
        zip(
            ("id", "labels", "image", "state", "memory_limit_bytes", "nano_cpus"),
            values,
            strict=True,
        )
    )


def discover() -> list[str]:
    candidates = docker(
        "ps", "--all", "--quiet", "--filter", "label=com.docker.compose.service=model"
    ).split()
    projects = set()
    for identifier in candidates:
        labels = inspect(identifier)["labels"]
        project = labels.get("com.docker.compose.project", "")
        compose = labels.get("com.docker.compose.project.config_files", "")
        if (
            re.fullmatch(r"ai-gateway-[0-9a-f]{12}", project)
            and "/setup-ai-gateway/" in compose
            and compose.endswith("/compose.yml")
        ):
            projects.add(project)
    if len(projects) != 1:
        raise RuntimeError(f"Expected one owned gateway project, found {len(projects)}")
    (project,) = projects
    identifiers = docker(
        "ps",
        "--all",
        "--quiet",
        "--filter",
        f"label=com.docker.compose.project={project}",
    ).split()
    services = [
        inspect(identifier)["labels"].get("com.docker.compose.service")
        for identifier in identifiers
    ]
    if sorted(services) != ["magpie", "model"]:
        raise RuntimeError("Owned gateway must have exactly model and magpie")
    return identifiers


def numbers(path: Path, delimiter: str | None = None) -> dict[str, int]:
    result = {}
    for line in path.read_text().splitlines():
        key, value = line.split(delimiter, 1) if delimiter else line.split(None, 1)
        result[key] = int(value.split()[0])
    return result


def memory(pid: int) -> dict:
    proc = Path("/proc") / str(pid)
    values = {}
    status = proc / "status"
    if status.exists():
        values["process_kib"] = {
            key: int(value.split()[0])
            for line in status.read_text().splitlines()
            if (key := line.split(":", 1)[0]) in {"VmRSS", "VmHWM"}
            for value in [line.split(":", 1)[1]]
        }
    cgroup = next(
        (
            line[3:]
            for line in (proc / "cgroup").read_text().splitlines()
            if line.startswith("0::")
        ),
        None,
    )
    if cgroup is None:
        raise RuntimeError("Unified cgroup memory path unavailable")
    base = Path("/sys/fs/cgroup").resolve()
    root = (base / cgroup.lstrip("/")).resolve()
    if not root.is_relative_to(base):
        raise RuntimeError("Invalid gateway cgroup path")
    for name in ("memory.current", "memory.peak", "memory.events", "memory.stat"):
        try:
            path = root / name
            values[name] = (
                numbers(path)
                if name in {"memory.events", "memory.stat"}
                else int(path.read_text())
            )
        except (OSError, ValueError) as error:
            values[name + ".error"] = type(error).__name__
    return values


def snapshot(identifiers: list[str], phase: str) -> dict:
    containers = []
    for identifier in identifiers:
        entry = inspect(identifier)
        entry["service"] = entry.pop("labels")["com.docker.compose.service"]
        pid = entry["state"]["Pid"]
        if pid:
            try:
                entry["memory"] = memory(pid)
            except (OSError, ValueError, RuntimeError) as error:
                entry["memory_error"] = str(error)
        containers.append(entry)
    return {
        "occurred_at_ms": time.time_ns() // 1_000_000,
        "phase": phase,
        "host_memory_kib": numbers(Path("/proc/meminfo"), ":"),
        "containers": containers,
    }


def main() -> int:
    output = Path(sys.argv[1])
    command = sys.argv[2:]
    if not command:
        raise ValueError("Supply an output path and the original test command")
    output.parent.mkdir(parents=True, exist_ok=True)
    identifiers: list[str] = []
    with output.open("w") as stream:

        def record(value: dict):
            stream.write(json.dumps(value) + "\n")
            stream.flush()

        try:
            identifiers = discover()
            metadata = list(
                Path(os.environ["RUNNER_TEMP"]).glob("ai-gateway-*/model.json")
            )
            if len(metadata) != 1:
                raise RuntimeError("Expected one gateway model metadata file")
            model = json.loads(metadata[0].read_text())
            record(
                {
                    "model": {
                        key: model[key]
                        for key in (
                            "repository",
                            "filename",
                            "revision",
                            "sha256",
                            "size",
                        )
                    }
                }
            )
        except (
            OSError,
            ValueError,
            KeyError,
            RuntimeError,
            subprocess.SubprocessError,
        ) as error:
            record({"observation_error": str(error), "phase": "discovery"})

        def capture(phase: str):
            if not identifiers:
                return
            try:
                value = snapshot(identifiers, phase)
                record(value)
                if phase in {"before", "after"}:
                    for container in value["containers"]:
                        current = container.get("memory", {})
                        print(
                            "Gateway resources",
                            phase,
                            container["service"],
                            "current_bytes",
                            current.get("memory.current"),
                            "peak_bytes",
                            current.get("memory.peak"),
                            "OOMKilled",
                            container["state"]["OOMKilled"],
                            flush=True,
                        )
            except (
                OSError,
                ValueError,
                KeyError,
                RuntimeError,
                subprocess.SubprocessError,
            ) as error:
                record({"observation_error": str(error), "phase": phase})

        capture("before")
        process = subprocess.Popen(command)
        try:
            while True:
                try:
                    returncode = process.wait(timeout=15)
                    break
                except subprocess.TimeoutExpired:
                    capture("running")
        finally:
            capture("after")
        record({"test_returncode": returncode})
        return returncode if returncode >= 0 else 128 - returncode


if __name__ == "__main__":
    raise SystemExit(main())
