"""Steer an active real Harness turn through the public API."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from collections import defaultdict
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
from provider import (
    HARNESS_ENTRY,
    STEERING_INPUT,
    steering_input,
    steering_result,
    steering_settled,
)

root = Path(sys.argv[1]).resolve()
workspace = Path(sys.argv[2]).resolve()
output = root / "steering"
output.mkdir()
os.environ.update(json.loads((root / "environment.private.json").read_text()))
events: list[AuditEvent] = []
recorded: defaultdict[str | None, asyncio.Event] = defaultdict(asyncio.Event)
exercised = False


async def record(ctx: Context, event: AuditEvent):
    events.append(event)

    def append():
        with (output / "audit.jsonl").open("a") as stream:
            stream.write(event.model_dump_json() + "\n")

    await asyncio.to_thread(append)
    if event.event_name == "test.turn.output":
        item = cast(dict[str, Any], event.payload)
        if item["payload"]["type"] == "completed":
            recorded[event.context.turn_id].set()


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
    global exercised
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

    async def verify(turn, expected):
        await recorded[turn.id].wait()
        sent = []
        settled = set()
        results = []
        for event in events:
            if event.context.turn_id != turn.id:
                continue
            if event.event_name in {"harness.source.sent", "harness.source.received"}:
                direction = event.event_name.removeprefix("harness.source.")
                packet = json.loads(cast(dict[str, Any], event.payload)["data"])
                if (value := steering_input(direction, packet)) is not None:
                    sent.append(value)
                if (value := steering_settled(direction, packet)) is not None:
                    settled.add(value)
                if (result := steering_result(direction, packet)) is not None:
                    results.append(result)
            if event.event_name == "test.turn.output":
                item = cast(dict[str, Any], event.payload)
                if item["payload"]["type"] == "completed":
                    assert set(sent) <= settled, (sent, settled)
        assert len(sent) == expected and len(set(sent)) == expected, sent
        assert results, turn.id
        return {"turn": turn.id, "inputs": len(sent), "results": results}

    try:
        burst = HarnessTurn(
            id=str(uuid7()), session_id=session.id, input_text=STEERING_INPUT
        )
        stream = await provider.start_turn(session, burst, Approvals(), timeout=30)
        for _ in range(16):
            assert await provider.steer_turn(session, burst, STEERING_INPUT, timeout=30)
        await consume(stream, burst)
        cases.append(await verify(burst, 17))
        assert not await provider.steer_turn(session, burst, STEERING_INPUT, timeout=30)

        continued = HarnessTurn(
            id=str(uuid7()), session_id=session.id, input_text=STEERING_INPUT
        )
        stream = await provider.start_turn(session, continued, Approvals(), timeout=30)
        await consume(stream, continued)
        cases.append(await verify(continued, 1))
        await asyncio.to_thread(
            (output / "result.json").write_text, json.dumps({"cases": cases}) + "\n"
        )
        await asyncio.to_thread(print, json.dumps({"cases": cases}), flush=True)
        exercised = True
        yield
    finally:
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
                    HARNESS_ENTRY,
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
