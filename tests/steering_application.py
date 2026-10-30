"""Exercise real native batching and result-boundary steering via public APIs."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast
from uuid import uuid7

from bazaar_contrib_audit import AUDIT, AUDIT_EVENT, AuditContext, AuditEvent, audit
from bazaar_contrib_harness import (
    HARNESS,
    HARNESS_INPUT,
    ApprovalRequest,
    ApprovalResult,
    HarnessInput,
    HarnessOutput,
    HarnessTurn,
    SessionInput,
)
from bazaar_core import (
    Composition,
    Context,
    Contrib,
    ContribEntry,
    ContribHub,
    ContribSpec,
    load,
)
from blockbuster import blockbuster_ctx

from bazaar_contrib_claude_code import harness

root = Path(sys.argv[1]).resolve()
workspace = Path(sys.argv[2]).resolve()
output = root / "steering"
output.mkdir()
os.environ.update(json.loads((root / "environment.private.json").read_text()))
events: list[AuditEvent] = []
gate_requested = False
gate_entered = asyncio.Event()
gate_released = asyncio.Event()
exercised = False


async def record(ctx: Context, event: AuditEvent):
    global gate_requested
    events.append(event)

    def append():
        with (output / "audit.jsonl").open("a") as stream:
            stream.write(event.model_dump_json() + "\n")

    await asyncio.to_thread(append)
    if event.event_name == "harness.source.received":
        packet = json.loads(cast(dict[str, Any], event.payload)["data"])
        if gate_requested and packet["type"] == "result":
            # Audit backpressure exposes a real result produced before the next
            # stdin writes. No protocol frame or provider is manufactured.
            gate_requested = False
            gate_entered.set()
            await gate_released.wait()


class Approvals:
    async def request_approval(
        self,
        request: ApprovalRequest,
        *,
        timeout: float,  # noqa: ASYNC109
    ) -> ApprovalResult:
        return ApprovalResult(request_id=request.request_id, decision="approved")


@asynccontextmanager
async def inputs(ctx: Context):
    ctx.offer(
        HARNESS_INPUT,
        HarnessInput(
            agent_id="steering",
            agent_name="Steering acceptance",
            workspace=workspace,
            startup_timeout=30,
            shutdown_timeout=5,
        ),
    )
    yield


@asynccontextmanager
async def exercise(ctx: Context):
    global gate_requested, exercised
    provider = ctx.accept(HARNESS)
    publisher = ctx.accept(AUDIT)
    started = await provider.start_session(
        SessionInput(id=str(uuid7()), workspace=workspace), timeout=30
    )
    assert started.value is not None, started
    session = started.value
    cases = []

    async def consume(stream, turn):
        items: list[HarnessOutput] = []
        async for item in stream:
            items.append(item)
            await publisher.publish(
                "test.turn.output",
                context=AuditContext(agent_id="steering", turn_id=turn.id),
                payload=item.model_dump(mode="json"),
            )
        await stream.aclose()
        assert sum(item.payload.type == "completed" for item in items) == 1, items
        assert not any(
            item.payload.type in {"failed", "unknown", "cancelled"} for item in items
        ), items

    def verify(turn, expected):
        selected = [event for event in events if event.context.turn_id == turn.id]
        sent = []
        terminal = set()
        results = []
        for event in selected:
            if event.event_name in {"harness.source.sent", "harness.source.received"}:
                packet = json.loads(cast(dict[str, Any], event.payload)["data"])
                if (
                    event.event_name == "harness.source.sent"
                    and packet["type"] == "user"
                ):
                    sent.append(packet["uuid"])
                if packet["type"] == "command_lifecycle" and packet["state"] in {
                    "completed",
                    "cancelled",
                    "discarded",
                    "refused",
                }:
                    terminal.add(packet["command_uuid"])
                if packet["type"] == "result":
                    results.append(
                        {
                            key: packet.get(key)
                            for key in (
                                "user_message_uuids",
                                "queued_turn_count",
                                "result_index",
                            )
                        }
                    )
            if event.event_name == "test.turn.output":
                item = cast(dict[str, Any], event.payload)
                if item["payload"]["type"] == "completed":
                    assert set(sent) <= terminal, (sent, terminal)
        assert len(sent) == expected and len(set(sent)) == expected, sent
        assert results, selected
        return {"turn": turn.id, "inputs": len(sent), "results": results}

    try:
        burst = HarnessTurn(id=str(uuid7()), session_id=session.id, input_text="/cost")
        stream = await provider.start_turn(session, burst, Approvals(), timeout=30)
        for _ in range(16):
            assert await provider.steer_turn(session, burst, "/cost", timeout=30)
        await consume(stream, burst)
        cases.append(verify(burst, 17))
        assert not await provider.steer_turn(session, burst, "/cost", timeout=30)

        boundary = HarnessTurn(
            id=str(uuid7()), session_id=session.id, input_text="/cost"
        )
        gate_requested = True
        stream = await provider.start_turn(session, boundary, Approvals(), timeout=30)
        consumer = asyncio.create_task(consume(stream, boundary))
        try:
            await gate_entered.wait()
            for _ in range(3):
                assert await provider.steer_turn(session, boundary, "/cost", timeout=30)
        finally:
            gate_released.set()
        await consumer
        cases.append(verify(boundary, 4))
        assert cases[-1]["results"][0]["queued_turn_count"] == 0, cases
        assert len(cases[-1]["results"]) >= 2, cases

        continued = HarnessTurn(
            id=str(uuid7()), session_id=session.id, input_text="/cost"
        )
        stream = await provider.start_turn(session, continued, Approvals(), timeout=30)
        await consume(stream, continued)
        cases.append(verify(continued, 1))
        await asyncio.to_thread(
            (output / "result.json").write_text, json.dumps({"cases": cases}) + "\n"
        )
        await asyncio.to_thread(print, json.dumps({"cases": cases}), flush=True)
        exercised = True
        yield
    finally:
        gate_released.set()
        await provider.stop_session(session, timeout=0)


async def main():
    timer = await asyncio.to_thread(load, (ContribSpec(contrib="timer-wheel"),))
    async with (
        ContribHub() as hub,
        hub.enable(
            Composition(
                entries=(
                    ContribEntry(name="audit", definition=audit),
                    *timer.entries,
                    ContribEntry(
                        name="recorder",
                        definition=Contrib(listen={AUDIT_EVENT: record}),
                    ),
                ),
                diagnostics=timer.diagnostics,
            )
        ),
        hub.enable(
            Composition(
                entries=(
                    ContribEntry(
                        name="inputs",
                        definition=Contrib(offers=[HARNESS_INPUT], lifespan=inputs),
                    ),
                    ContribEntry(name="claude", definition=harness),
                    ContribEntry(
                        name="exercise",
                        definition=Contrib(accepts=[HARNESS, AUDIT], lifespan=exercise),
                    ),
                )
            )
        ),
    ):
        pass
    assert exercised


with blockbuster_ctx():
    asyncio.run(main())
