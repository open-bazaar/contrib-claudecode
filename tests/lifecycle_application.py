"""Audit a real CLI session stopped with the application's zero-second budget."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast
from uuid import uuid7

import psutil
from bazaar_contrib_audit import AUDIT, AUDIT_EVENT, AuditContext, AuditEvent, audit
from bazaar_contrib_harness import (
    HARNESS,
    HARNESS_INPUT,
    HarnessInput,
    HarnessSourceEvent,
    SessionInput,
)
from bazaar_contrib_timer import TIMER
from bazaar_contrib_timer import timeout as timer_timeout
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
output = root / "lifecycle"
output.mkdir()
os.environ.update(json.loads((root / "environment.private.json").read_text()))
events: list[AuditEvent] = []
race_session_id: str | None = None
close_entered = asyncio.Event()
close_released = asyncio.Event()
close_cancelled = False
cancelled_stop = False


async def record(ctx: Context, event: AuditEvent):
    global close_cancelled
    if (
        event.event_name == "harness.source.closed"
        and event.context.session_id == race_session_id
    ):
        close_entered.set()
        try:
            await close_released.wait()
        except asyncio.CancelledError:
            close_cancelled = True
            raise
    events.append(event)

    def append():
        with (output / "audit.jsonl").open("a") as stream:
            stream.write(event.model_dump_json() + "\n")

    await asyncio.to_thread(append)


@asynccontextmanager
async def inputs(ctx: Context):
    ctx.offer(
        HARNESS_INPUT,
        HarnessInput(
            agent_id="lifecycle",
            agent_name="Lifecycle acceptance",
            workspace=workspace,
            startup_timeout=30,
            shutdown_timeout=5,
        ),
    )
    yield


@asynccontextmanager
async def exercise(ctx: Context):
    global race_session_id, cancelled_stop
    provider = ctx.accept(HARNESS)
    publisher = ctx.accept(AUDIT)
    result = await provider.start_session(
        SessionInput(id=str(uuid7()), workspace=workspace), timeout=30
    )
    assert result.value is not None, result
    await provider.stop_session(result.value, timeout=0)
    await publisher.publish(
        "test.session.stopped",
        context=AuditContext(agent_id="lifecycle"),
        payload={"budget": 0},
    )

    race_session_id = str(uuid7())
    result = await provider.start_session(
        SessionInput(id=race_session_id, workspace=workspace), timeout=30
    )
    assert result.value is not None, result
    session = result.value
    processes = await asyncio.to_thread(
        lambda: [
            process
            for process in psutil.Process().children(recursive=True)
            if f"--session-id={session.provider_thread_id}" in process.cmdline()
        ]
    )
    assert len(processes) == 1, [process.pid for process in processes]
    await asyncio.to_thread(processes[0].terminate)
    try:
        # The real watcher is publishing its close when the owner stops it.
        await close_entered.wait()
        await provider.stop_session(session, timeout=0)
    finally:
        close_released.set()

    result = await provider.start_session(
        SessionInput(id=str(uuid7()), workspace=workspace), timeout=30
    )
    assert result.value is not None, result
    try:
        async with timer_timeout(ctx.accept(TIMER), 0):
            await provider.stop_session(result.value, timeout=600)
    except TimeoutError:
        cancelled_stop = True
    yield


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
    ):
        async with hub.enable(
            Composition(
                entries=(
                    ContribEntry(
                        name="inputs",
                        definition=Contrib(offers=[HARNESS_INPUT], lifespan=inputs),
                    ),
                    ContribEntry(name="claude", definition=harness),
                    ContribEntry(
                        name="exercise",
                        definition=Contrib(
                            accepts=[HARNESS, AUDIT, TIMER], lifespan=exercise
                        ),
                    ),
                )
            )
        ):
            pass
        messages = [
            HarnessSourceEvent.model_validate(event.payload)
            for event in events
            if event.event_name in {"harness.source.sent", "harness.source.received"}
        ]
        closed = [
            cast(dict[str, Any], event.payload)
            for event in events
            if event.event_name == "harness.source.closed"
        ]
        live = await asyncio.to_thread(
            lambda: [
                process.pid
                for process in psutil.Process().children(recursive=True)
                if process.is_running() and process.status() != psutil.STATUS_ZOMBIE
            ]
        )
        report = {
            "source_messages": len(messages),
            "closed": closed,
            "close_audit_cancelled": close_cancelled,
            "cancelled_stop": cancelled_stop,
            "live_owned_children": live,
        }
        await asyncio.to_thread(
            (output / "result.json").write_text, json.dumps(report) + "\n"
        )
        await asyncio.to_thread(print, json.dumps(report), flush=True)
        assert messages and len(closed) == 3 and not close_cancelled, report
        assert cancelled_stop, report
        assert {message.source_id for message in messages} == {
            source["source_id"] for source in closed
        }, report
        for source in closed:
            assert [
                message.sequence
                for message in messages
                if message.source_id == source["source_id"]
            ] == list(range(1, source["sequence"] + 1)), report
        assert not live, report


with blockbuster_ctx():
    asyncio.run(main())
