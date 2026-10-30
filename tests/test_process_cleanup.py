"""A cancelled stop must retain a real detached descendant until cleanup."""

from __future__ import annotations

import asyncio
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import psutil
from bazaar_contrib_audit import AUDIT, AUDIT_EVENT, AuditContext, AuditEvent, audit
from bazaar_core import Composition, Context, Contrib, ContribEntry, ContribHub

from bazaar_contrib_claude_code.children import Children
from bazaar_contrib_claude_code.process import ProcessSpec, ProcessSupervisor
from bazaar_contrib_claude_code.source import SourceAudit


async def test_cancelled_stop_cleans_detached_descendant(tmp_path: Path):
    ready = asyncio.Event()
    events: list[AuditEvent] = []
    descendants: list[psutil.Process] = []
    survived: list[int] = []
    cancelled = False

    async def record(ctx: Context, event: AuditEvent):
        events.append(event)
        if event.event_name == "harness.source.received":
            payload = cast(dict[str, Any], event.payload)
            descendants.append(psutil.Process(int(payload["data"].strip())))
            ready.set()

    @asynccontextmanager
    async def exercise(ctx: Context):
        nonlocal cancelled
        children = Children()
        script = (
            "import os,subprocess,sys;"
            "group={'creationflags':subprocess.CREATE_NEW_PROCESS_GROUP}"
            " if os.name=='nt' else {'start_new_session':True};"
            "child=subprocess.Popen([sys.executable,'-c','import threading;threading.Event().wait()'],"
            "stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,"
            "**group);"
            "print(child.pid,flush=True);"
            "sys.stdin.read()"
        )
        supervisor = ProcessSupervisor(
            ProcessSpec(sys.executable, ("-c", script), tmp_path, dict(os.environ)),
            children,
            source=SourceAudit(
                ctx.accept(AUDIT),
                AuditContext(
                    agent_id="process-cleanup",
                    harness="claudecode",
                    session_id=str(uuid4()),
                ),
            ),
        )
        try:
            await supervisor.start(timeout=30)
            await ready.wait()
            stop = asyncio.create_task(supervisor.stop(timeout=600))
            asyncio.get_running_loop().call_soon(stop.cancel)
            try:
                await stop
            except asyncio.CancelledError:
                cancelled = True
            await children.end_all()
            survived.extend(
                descendant.pid
                for descendant in descendants
                if descendant.is_running()
                and descendant.status() != psutil.STATUS_ZOMBIE
            )
            yield
        finally:
            if supervisor.is_running:
                await supervisor.stop(timeout=0)
            await children.end_all()
            for descendant in descendants:
                try:
                    descendant.kill()
                except psutil.NoSuchProcess:
                    pass
            await asyncio.to_thread(psutil.wait_procs, descendants)

    async with (
        ContribHub() as hub,
        hub.enable(
            Composition(
                entries=(
                    ContribEntry(name="audit", definition=audit),
                    ContribEntry(
                        name="recorder",
                        definition=Contrib(listen={AUDIT_EVENT: record}),
                    ),
                )
            )
        ),
        hub.enable(
            Composition(
                entries=(
                    ContribEntry(
                        name="exercise",
                        definition=Contrib(accepts=[AUDIT], lifespan=exercise),
                    ),
                )
            )
        ),
    ):
        pass
    assert cancelled and len(descendants) == 1
    assert not survived
    closed = [
        cast(dict[str, Any], event.payload)
        for event in events
        if event.event_name == "harness.source.closed"
    ]
    assert len(closed) == 1 and closed[0]["sequence"] == 1
