"""Observe a real Claude Code turn using bcc in an isolated installed application."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import socket
import sys
import tomllib
from http.client import HTTPConnection
from itertools import pairwise
from pathlib import Path
from typing import Any, cast
from uuid import uuid7

import psutil
import tomli_w
from bazaar_contrib_audit import AuditEvent
from bazaar_contrib_channel import ChannelApprovalRequest, ChannelTarget, SenderIdentity
from bazaar_contrib_harness import (
    ApprovalResult,
    ApprovalScope,
    HarnessSourceEvent,
)
from bcn_claude_test_channel import ChannelMessage
from pydantic import TypeAdapter

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("root", type=Path)
parser.add_argument(
    "scenario",
    choices=(
        "mcp-once",
        "bcc",
        "full-access",
        "recovery",
        "refresh",
        "background",
    ),
)
parser.add_argument("workspace", type=Path)
arguments = parser.parse_args()
root = arguments.root.resolve()
scenario = arguments.scenario
command = Path(__file__).with_name("run_application.py")
MCP_FEEDBACK_LIMIT = 8
AUDIT_EVENT = TypeAdapter[AuditEvent](AuditEvent)


async def exercise():
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
    } | json.loads((root / "environment.private.json").read_text())
    log = (root / "node.log").open("wb")
    process = None
    writer = None
    collector = None
    queue = asyncio.Queue()
    phase = "startup"
    workspace = arguments.workspace.resolve()
    background = None
    preview_processes = {}
    if scenario == "background":
        config = root / "node.toml"
        document = tomllib.loads(config.read_text())
        for package in document["contribs"]:
            for entry in package["enabled"]:
                if entry["entry"] == "agent":
                    for agent in entry["config"]["agents"]:
                        agent["idle_timeout"] = 2
        config.write_text(tomli_w.dumps(document))

    def inspections():
        path = root / "audit.jsonl"
        if not path.exists():
            return []
        return [
            audit.payload
            for line in path.read_text().splitlines()
            for audit in (AUDIT_EVENT.validate_json(line),)
            if audit.event_name == "test_channel.inspection"
        ]

    async def packets(reader):
        pending = bytearray()
        while chunk := await reader.read(8192):
            pending.extend(chunk)
            while (end := pending.find(b"\n")) >= 0:
                line = pending[:end]
                del pending[: end + 1]
                yield json.loads(line)
        if pending:
            yield json.loads(pending)

    async def collect(reader):
        async for packet in packets(reader):
            if "audit" in packet:
                print(json.dumps({"phase": phase, **packet}), flush=True)
            await queue.put(packet)
        await queue.put(None)

    async def start():
        nonlocal process, writer, collector, queue, phase
        phase = "startup"
        endpoint = workspace / "channel.json"
        endpoint.unlink(missing_ok=True)
        queue = asyncio.Queue()
        inspected = len(await asyncio.to_thread(inspections))
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            str(command),
            str(root / "node.toml"),
            cwd=root,
            env=environment,
            stdin=asyncio.subprocess.PIPE,
            stdout=log,
            stderr=log,
        )
        print(json.dumps({"node_started": process.pid}), flush=True)
        while not endpoint.exists():
            if process.returncode is not None:
                raise RuntimeError("The isolated node exited; inspect node.log")
            await asyncio.sleep(0.05)
        while len(found := await asyncio.to_thread(inspections)) == inspected:
            if process.returncode is not None:
                raise RuntimeError("The isolated node exited; inspect node.log")
            await asyncio.sleep(0.05)
        inspection = cast(dict[str, Any], found[-1])
        assert inspection["available"] and inspection["version"], inspection
        assert inspection["models"] and not inspection["error"], inspection
        reader, writer = await asyncio.open_connection(
            "127.0.0.1", json.loads(endpoint.read_text())["port"]
        )
        collector = asyncio.create_task(collect(reader))

    async def stop():
        nonlocal phase
        phase = "shutdown"
        if process is None:
            return
        try:
            children = await asyncio.to_thread(
                psutil.Process(process.pid).children, recursive=True
            )
        except psutil.NoSuchProcess:
            children = []
        if process.returncode is None:
            assert process.stdin is not None
            process.stdin.close()
        await process.wait()
        if collector is not None:
            await collector
        if writer is not None:
            writer.close()
            await writer.wait_closed()
        alive = []
        details = []
        for child in children:
            try:
                if child.is_running() and child.status() != psutil.STATUS_ZOMBIE:
                    details.append(
                        child.as_dict(
                            attrs=["pid", "ppid", "name", "cmdline", "status"]
                        )
                    )
                    alive.append(child.pid)
            except psutil.NoSuchProcess:
                pass
        record = {
            "node_pid": process.pid,
            "claude_config_dir": environment["CLAUDE_CONFIG_DIR"],
            "node_returncode": process.returncode,
            "live_owned_children": alive,
            "live_owned_processes": details,
        }
        print(json.dumps({"node_stopped": record}), flush=True)
        assert process.returncode == 0, record
        assert not alive, record
        sources: dict[str, list[HarnessSourceEvent]] = {}
        closed: dict[str, dict[str, Any]] = {}
        for line in (root / "audit.jsonl").read_text().splitlines():
            audit = AUDIT_EVENT.validate_json(line)
            if audit.event_name in {"harness.source.sent", "harness.source.received"}:
                source = HarnessSourceEvent.model_validate(audit.payload)
                sources.setdefault(source.source_id, []).append(source)
                assert audit.event_name == f"harness.source.{source.direction}"
                assert audit.context.session_id
                assert audit.observed_at_ms >= source.occurred_at_ms
            elif audit.event_name == "harness.source.closed":
                payload = cast(dict[str, Any], audit.payload)
                closed[payload["source_id"]] = payload
        assert sources and set(sources) == set(closed)
        source_coverage = {}
        for source_id, messages in sources.items():
            messages.sort(key=lambda event: event.sequence)
            assert [event.sequence for event in messages] == list(
                range(1, closed[source_id]["sequence"] + 1)
            )
            directions = {event.direction for event in messages}
            assert directions == {"sent", "received"}
            assert all(
                previous.monotonic_ns <= current.monotonic_ns
                for previous, current in pairwise(messages)
            )
            source_coverage[source_id] = {
                "messages": len(messages),
                "sent": sum(event.direction == "sent" for event in messages),
                "received": sum(event.direction == "received" for event in messages),
                "closed": closed[source_id],
                "methods": sorted(
                    {
                        packet["method"]
                        for event in messages
                        for packet in (json.loads(event.data),)
                        if packet.get("method")
                    }
                ),
            }
        print(json.dumps({"source_audit_coverage": source_coverage}), flush=True)

    try:
        await start()

        async def inbound(body):
            assert writer is not None
            message = ChannelMessage(
                id=str(uuid7()),
                target=ChannelTarget(kind="dm", thread_id="tester"),
                sender=SenderIdentity(id="tester", name="Tester"),
                mentions_agent=True,
                body=body,
                metadata={"review": "approved"},
            )
            writer.write(
                json.dumps({"inbound": message.model_dump(mode="json")}).encode()
                + b"\n"
            )
            await writer.drain()
            return message.id

        async def finish(
            label,
            steer=False,
            stop_at_started=False,
            reject=False,
            scope=ApprovalScope.ONCE,
            mcp=False,
            input_message_id=None,
        ):
            nonlocal phase
            assert writer is not None
            phase = label
            print(json.dumps({"phase_start": label}), flush=True)
            calls = []
            outbound = []
            interactions = []
            tools = []
            started_tools = []
            accepted_steering = []
            approval_requests = []
            approval_decisions = []
            steered = False
            terminal = None
            completion = None
            canonical_message_id = None
            requested_turn_id = None
            entered = input_message_id is None
            previous_items = set()
            commands = {}
            while (packet := await queue.get()) is not None:
                audit = packet.get("audit", {})
                if (
                    audit.get("event_name") == "channel.received"
                    and audit["payload"].get("provider_message_id") == input_message_id
                ):
                    canonical_message_id = audit["payload"]["id"]
                event = packet.get("event", {})
                payload = event.get("payload", {})
                call = payload.get("call", {})
                if (
                    audit.get("event_name") == "harness.turn.requested"
                    and canonical_message_id
                ):
                    turn = audit["payload"]
                    if turn["id"] == (
                        "turn-" + canonical_message_id
                    ) or canonical_message_id[:8] in re.findall(
                        r"(?:first|latest) msg=([a-f0-9-]+)", turn["input_text"]
                    ):
                        requested_turn_id = turn["id"]
                if audit.get("event_name") == "harness.turn.steer.accepted":
                    accepted_steering.append(audit["context"])
                    if canonical_message_id:
                        entered = True
                if requested_turn_id and event.get("turn_id") == requested_turn_id:
                    entered = True
                if audit.get("event_name") == "approval.requested":
                    approval_requests.append(audit["payload"])
                elif audit.get("event_name") == "approval.decided":
                    approval_decisions.append(audit["payload"])
                assert payload.get("type") != "compaction.started", (
                    f"Context compaction entered during {label}; inspect the printed events"
                )
                if (
                    steer
                    and not steered
                    and payload.get("type") in {"content_delta", "tool.started"}
                ):
                    await inbound("Please keep your reply brief.")
                    steered = True
                if "outbound" in packet:
                    writer.write(json.dumps({"id": packet["id"]}).encode() + b"\n")
                    await writer.drain()
                    outbound.append(packet["outbound"])
                if "approval" in packet:
                    request = ChannelApprovalRequest.model_validate(packet["approval"])
                    decision = ApprovalResult(
                        request_id=request.approval.request_id,
                        decision="rejected"
                        if reject and not interactions
                        else "approved",
                        scope=scope
                        if scope in request.approval.allowed_scopes
                        else ApprovalScope.ONCE,
                    )
                    writer.write(
                        json.dumps(
                            {
                                "id": packet["id"],
                                "decision": decision.model_dump(mode="json"),
                            }
                        ).encode()
                        + b"\n"
                    )
                    await writer.drain()
                    interactions.append(
                        {
                            "request": request.model_dump(mode="json"),
                            "decision": decision.model_dump(mode="json"),
                        }
                    )
                if payload.get("type") == "tool.started":
                    if entered:
                        started_tools.append({"kind": payload["type"], "call": call})
                    else:
                        previous_items.add(call["call_id"])
                    command = (
                        call["input"].get("command")
                        if isinstance(call["input"], dict)
                        else None
                    )
                    if isinstance(command, str) and re.search(
                        r"\bbcc(?:\.cmd)?(?=[\s\"']|$)", command
                    ):
                        commands[call["call_id"]] = command
                if (
                    entered
                    and call.get("call_id") not in previous_items
                    and payload.get("type") in {"tool.completed", "tool.failed"}
                ):
                    tools.append({"kind": payload["type"], "call": call})
                if (
                    payload.get("type") == "tool.completed"
                    and call.get("call_id") in commands
                ):
                    calls.append(commands[call["call_id"]])
                if (
                    entered
                    and mcp
                    and call.get("call_id") not in previous_items
                    and payload.get("type") in {"tool.completed", "tool.failed"}
                    and call.get("name") == "mcp__test__get_value"
                ):
                    terminal = event
                    completion = "mcp_result"
                    break
                if entered and (
                    payload.get("type")
                    in {"completed", "cancelled", "failed", "unknown"}
                    or (stop_at_started and payload.get("type") == "started")
                ):
                    terminal = event
                    completion = "turn_event"
                    break
            assert terminal, (
                "The channel closed before the requested state was observed"
            )
            if completion != "mcp_result":
                assert terminal["payload"]["type"] == (
                    "started" if stop_at_started else "completed"
                ), terminal
            record = {
                "phase": label,
                "bcc_calls": calls,
                "outbound": outbound,
                "interactions": interactions,
                "tools": tools,
                "started_tools": started_tools,
                "session": terminal["session_id"],
                "provider_thread": terminal["provider_session_id"],
                "provider_turn": terminal["provider_turn_id"],
                "state": terminal["payload"]["type"],
                "completion": completion,
                "input_message_id": input_message_id,
                "canonical_message_id": canonical_message_id,
                "input_entered_native_turn": entered,
                "previous_item_ids": sorted(previous_items),
                "steered": steered,
                "accepted_steering": accepted_steering,
                "approval_requests": approval_requests,
                "approval_decisions": approval_decisions,
            }
            print(json.dumps({"phase_complete": label, **record}), flush=True)
            return record

        def verify_value(record, arguments, scope=None):
            """Check native MCP execution returned the real server's value."""
            calls = [
                tool
                for tool in record["tools"]
                if tool["call"].get("name") == "mcp__test__get_value"
            ]
            assert calls and all(tool["kind"] == "tool.completed" for tool in calls), (
                record
            )
            assert any(
                started["call"]["input"] == arguments
                and started["call"]["call_id"] == tool["call"]["call_id"]
                for started in record["started_tools"]
                for tool in calls
            )
            assert any(
                json.loads(tool["call"]["output"])
                == {"key": arguments["key"], "value": "MCP_OK"}
                for tool in calls
            ), record
            interactions = [
                interaction
                for interaction in record["interactions"]
                if interaction["request"]["approval"]["action"]
                == "mcp__test__get_value"
            ]
            if scope is None:
                assert not interactions, interactions
            else:
                assert interactions and all(
                    interaction["decision"]["decision"] == "approved"
                    and interaction["decision"]["scope"] == scope.value
                    and scope.value
                    in interaction["request"]["approval"]["allowed_scopes"]
                    for interaction in interactions
                ), record

        async def query_value(label, key, scope=None):
            """Observe the MCP result without waiting for a natural turn ending."""
            nonlocal phase
            phase = label
            arguments = {"key": key}
            request = (
                "请使用工具调用 mcp__test__get_value，"
                f"参数为 {json.dumps(arguments)}，读取测试值。"
            )
            input_message_id = await inbound(request)
            context = None
            retry = 0
            while True:
                phase = label if retry == 0 else f"{label}-retry-{retry}"
                record = await finish(
                    phase,
                    scope=scope or ApprovalScope.ONCE,
                    mcp=True,
                    input_message_id=input_message_id,
                )
                actual_context = (record["session"], record["provider_thread"])
                if context is None:
                    context = actual_context
                assert actual_context == context, record
                called = any(
                    tool["call"].get("name") == "mcp__test__get_value"
                    for tool in record["started_tools"] + record["tools"]
                )
                approval_requested = any(
                    interaction["request"]["approval"]["action"]
                    == "mcp__test__get_value"
                    for interaction in record["interactions"]
                )
                if called or approval_requested or retry == MCP_FEEDBACK_LIMIT:
                    verify_value(record, arguments, scope)
                    return record
                retry += 1
                feedback = (
                    "还没有收到实际工具结果。请使用工具调用 mcp__test__get_value，"
                    f"参数为 {json.dumps(arguments)}，读取测试值。"
                )
                print(
                    json.dumps({"phase": label, "retry": retry, "feedback": feedback}),
                    flush=True,
                )
                phase = f"{label}-retry-{retry}"
                input_message_id = await inbound(feedback)

        if scenario == "mcp-once":
            first = await query_value(
                "mcp-once",
                "once",
                ApprovalScope.ONCE,
            )
        elif scenario == "bcc":
            await inbound("Please remind me to drink some water in five seconds.")
            first = await finish("bcc", steer=True)
            assert first["bcc_calls"], (
                "No successful bcc call was observed; inspect the printed events"
            )
            assert any(
                interaction["request"]["approval"]["action"] == "Bash"
                for interaction in first["interactions"]
            ), "No native command approval reached the channel"
            assert first["steered"] and first["accepted_steering"]

            await inbound("What reminder did I ask you to set? Please reply to me.")
            continued = await finish("continued", reject=True)
            assert (
                continued["session"] == first["session"]
                and continued["provider_thread"] == first["provider_thread"]
            )
            rejected = {
                interaction["request"]["approval"]["request_id"]
                for interaction in continued["interactions"]
                if interaction["decision"]["decision"] == "rejected"
            }
            assert rejected and any(
                tool["kind"] == "tool.failed" and tool["call"]["call_id"] in rejected
                for tool in continued["tools"]
            ), continued

        elif scenario == "full-access":
            note = workspace / "full-access-note.txt"
            await inbound(
                "I'm reading this chat on my phone and cannot view our workspace. "
                "Please save a short note about drinking enough water to "
                "full-access-note.txt in the workspace, then send me a message "
                "here with the filename and a one-sentence summary."
            )
            first = await finish("full-access")
            assert (
                note.read_text().strip() and first["outbound"] and first["bcc_calls"]
            ), first

        elif scenario == "recovery":
            assert process is not None
            await inbound("Please remember that I prefer brief replies.")
            first = await finish("initial")
            parent = psutil.Process(process.pid)

            def sessions():
                return [
                    child
                    for child in parent.children()
                    if any(
                        value.startswith(("--session-id=", "--resume="))
                        for value in child.cmdline()
                    )
                    and child.environ().get("CLAUDE_CONFIG_DIR")
                    == environment["CLAUDE_CONFIG_DIR"]
                ]

            servers = await asyncio.to_thread(sessions)
            assert len(servers) == 1, [child.cmdline() for child in servers]
            server = servers[0]
            owned = [server, *await asyncio.to_thread(server.children, recursive=True)]
            print(
                json.dumps(
                    {
                        "recovery_process": {
                            "pid": server.pid,
                            "ppid": server.ppid(),
                            "cmdline": server.cmdline(),
                        }
                    }
                ),
                flush=True,
            )
            for child in reversed(owned):
                try:
                    await asyncio.to_thread(child.terminate)
                except psutil.NoSuchProcess:
                    pass
            await asyncio.to_thread(psutil.wait_procs, owned)
            await inbound("What reply style did I ask for? Please reply to me.")
            recovered = await finish("recovered")
            assert recovered["session"] == first["session"], recovered
            assert recovered["provider_thread"] == first["provider_thread"], recovered
            current = await asyncio.to_thread(sessions)
            assert len(current) == 1 and current[0].pid != server.pid
            await inbound("Tell me briefly what you can help me with.")
            await finish("stop-active", stop_at_started=True)

        elif scenario == "refresh":
            await inbound("Please remember that I prefer brief replies.")
            first = await finish("initial")
            for name in ("AGENTS.md", "CLAUDE.md"):
                (workspace / name).write_text("Keep replies concise.\n")
                await asyncio.sleep(1)
                await inbound("What reply style did I ask for? Please reply to me.")
                refreshed = await finish("refreshed-" + name)
                assert refreshed["session"] != first["session"]
                assert refreshed["provider_thread"] != first["provider_thread"]
                first = refreshed

        elif scenario == "background":
            assert process is not None
            with socket.socket() as reserve:
                reserve.bind(("127.0.0.1", 0))
                port = reserve.getsockname()[1]
            filename = f"preview-{uuid7()}.txt"
            expected = f"BCN preview {uuid7()}\n"
            (workspace / filename).write_text(expected)
            background = {"port": port, "file": filename, "idle_timeout_seconds": 2}

            def preview():
                connection = HTTPConnection("127.0.0.1", port, timeout=10)
                try:
                    connection.request("GET", "/" + filename)
                    response = connection.getresponse()
                    content = response.read().decode()
                    assert response.status == 200 and content == expected, (
                        response.status,
                        content,
                    )
                    return {"status": response.status, "body": content}
                finally:
                    connection.close()

            def processes():
                assert process is not None
                children = psutil.Process(process.pid).children(recursive=True)
                records = []
                listeners = []
                for child in children:
                    try:
                        records.append(
                            {
                                "pid": child.pid,
                                "ppid": child.ppid(),
                                "command": child.cmdline(),
                            }
                        )
                        for item in child.net_connections(kind="tcp"):
                            if (
                                item.status == psutil.CONN_LISTEN
                                and item.laddr.port == port
                            ):
                                preview_processes[child.pid] = child
                                listeners.append(
                                    {
                                        "pid": child.pid,
                                        "address": item.laddr.ip,
                                        "port": port,
                                    }
                                )
                    except psutil.NoSuchProcess:
                        continue
                assert listeners and all(
                    item["address"] == "127.0.0.1" for item in listeners
                ), records
                return {
                    "node_pid": process.pid,
                    "children": records,
                    "listeners": listeners,
                }

            initial_id = await inbound(
                "Please start a local HTTP preview for the current folder. Run "
                f"`python3 -m http.server {port} --bind 127.0.0.1` with the "
                "Bash tool's `run_in_background` option, without a trailing "
                "`&`. Keep that background task running after this turn. "
                "Check that the preview responds, then use `bcc message send` "
                "to send its address to this conversation."
            )
            first = await finish("background-start", input_message_id=initial_id)
            assert first["outbound"], "The preview address was not sent to the channel"
            background["initial"] = first
            background["initial_http"] = await asyncio.to_thread(preview)
            background["initial_processes"] = await asyncio.to_thread(processes)
            # Let the real Agent's idle expiry ask its Harness about background
            # work. A live preview must prevent the node from closing this session.
            await asyncio.sleep(3)
            background["after_idle_http"] = await asyncio.to_thread(preview)
            background["after_idle_processes"] = await asyncio.to_thread(processes)
            followup_id = await inbound(
                "Is the preview still available? Please check it, keep the "
                "existing server running, and use `bcc message send` to send "
                "its address to this conversation."
            )
            continued = await finish(
                "background-continued", input_message_id=followup_id
            )
            assert continued["session"] == first["session"]
            assert continued["provider_thread"] == first["provider_thread"]
            assert continued["outbound"], "The follow-up was not sent to the channel"
            background["continued"] = continued
            background["continued_http"] = await asyncio.to_thread(preview)
            background["continued_processes"] = await asyncio.to_thread(processes)
            await inbound("Please check the preview once more and leave it running.")
            active = await finish("background-active-close", stop_at_started=True)
            assert active["session"] == first["session"]
            assert active["provider_thread"] == first["provider_thread"]
            background["active_close"] = active

    finally:
        await stop()
        if background is not None:
            try:
                _reader, preview_writer = await asyncio.open_connection(
                    "127.0.0.1", background["port"]
                )
            except OSError:
                background["listening_after_node_stop"] = False
            else:
                preview_writer.close()
                await preview_writer.wait_closed()
                background["listening_after_node_stop"] = True
            alive = []
            for child in preview_processes.values():
                try:
                    if child.is_running() and child.status() != psutil.STATUS_ZOMBIE:
                        alive.append(child.pid)
                except psutil.NoSuchProcess:
                    pass
            background["live_preview_processes_after_node_stop"] = alive
            print(json.dumps({"background": background}), flush=True)
            assert not background["listening_after_node_stop"], background
            assert not background["live_preview_processes_after_node_stop"], background
        log.close()


if __name__ == "__main__":
    asyncio.run(exercise())
