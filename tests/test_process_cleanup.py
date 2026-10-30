"""Real descendants stay owned through parent exit and stop cancellation."""

from __future__ import annotations

import asyncio
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import psutil
import pytest
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
    exercised = False

    async def record(ctx: Context, event: AuditEvent):
        events.append(event)
        if event.event_name == "harness.source.received":
            payload = cast(dict[str, Any], event.payload)
            descendants.append(
                await asyncio.to_thread(psutil.Process, int(payload["data"].strip()))
            )
            ready.set()

    @asynccontextmanager
    async def exercise(ctx: Context):
        nonlocal cancelled, exercised
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
                await asyncio.to_thread(
                    lambda: [
                        descendant.pid
                        for descendant in descendants
                        if descendant.is_running()
                        and descendant.status() != psutil.STATUS_ZOMBIE
                    ]
                )
            )
            exercised = True
            yield
        finally:
            if supervisor.is_running:
                await supervisor.stop(timeout=0)
            await children.end_all()
            for descendant in descendants:
                try:
                    await asyncio.to_thread(descendant.kill)
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
    assert exercised and cancelled and len(descendants) == 1
    assert not survived
    closed = [
        cast(dict[str, Any], event.payload)
        for event in events
        if event.event_name == "harness.source.closed"
    ]
    assert len(closed) == 1 and closed[0]["sequence"] == 1


@pytest.mark.skipif(sys.platform != "win32", reason="Windows Job ownership")
@pytest.mark.parametrize("orphaned", [False, True], ids=["child", "grandchild"])
@pytest.mark.parametrize("ending", ["exit", "stop", "zero", "cancel"])
async def test_windows_job_cleans_inherited_pipes(
    tmp_path: Path, orphaned: bool, ending: str
):
    ready = asyncio.Event()
    closed = asyncio.Event()
    events: list[AuditEvent] = []
    descendants: list[psutil.Process] = []
    root_ready = False
    exercised = False

    async def record(ctx: Context, event: AuditEvent):
        nonlocal root_ready
        events.append(event)
        if event.event_name == "harness.source.closed":
            closed.set()
        elif event.event_name == "harness.source.received":
            payload = cast(dict[str, Any], event.payload)
            data = payload["data"].strip()
            if data.startswith("leaf:"):
                descendants.append(
                    await asyncio.to_thread(
                        psutil.Process, int(data.removeprefix("leaf:"))
                    )
                )
            elif data == "root-ready":
                root_ready = True
            if descendants and root_ready:
                ready.set()

    @asynccontextmanager
    async def exercise(ctx: Context):
        nonlocal exercised
        children = Children()
        leaf = (
            "import os,sys,threading;"
            "print('leaf stderr',file=sys.stderr,flush=True);"
            "print('leaf:'+str(os.getpid()),flush=True);"
            "threading.Event().wait()"
        )
        descendant = leaf
        if orphaned:
            descendant = (
                "import subprocess,sys;"
                f"subprocess.Popen([sys.executable,'-c',{leaf!r}],"
                "stdin=subprocess.DEVNULL,"
                "creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)"
            )
        script = (
            "import subprocess,sys;"
            f"child=subprocess.Popen([sys.executable,'-c',{descendant!r}],"
            "stdin=subprocess.DEVNULL,"
            "creationflags=subprocess.CREATE_NEW_PROCESS_GROUP);"
            + ("assert child.wait()==0;" if orphaned else "")
            + "print('root-ready',flush=True);"
            "sys.exit(7 if sys.stdin.readline() else 0)"
        )
        supervisor = ProcessSupervisor(
            ProcessSpec(sys.executable, ("-c", script), tmp_path, dict(os.environ)),
            children,
            source=SourceAudit(
                ctx.accept(AUDIT),
                AuditContext(
                    agent_id="windows-process-cleanup",
                    harness="claudecode",
                    session_id=str(uuid4()),
                ),
            ),
        )
        try:
            await supervisor.start(timeout=30)
            await ready.wait()
            if ending == "exit":
                await supervisor.send(
                    {"type": "user", "message": {"role": "user", "content": "exit"}}
                )
                assert await supervisor.wait() == 7
            elif ending == "cancel":
                stop = asyncio.create_task(supervisor.stop(timeout=600))
                asyncio.get_running_loop().call_soon(stop.cancel)
                with pytest.raises(asyncio.CancelledError):
                    await stop
            else:
                await supervisor.stop(timeout=0 if ending == "zero" else 30)
            await children.end_all()
            await closed.wait()
            assert len(descendants) == 1
            await asyncio.to_thread(psutil.wait_procs, descendants)
            assert not await asyncio.to_thread(descendants[0].is_running)
            exercised = True
            yield
        finally:
            if supervisor.is_running:
                await supervisor.stop(timeout=0)
            await children.end_all()
            for descendant_process in descendants:
                try:
                    await asyncio.to_thread(descendant_process.kill)
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
    assert exercised
    closures = [
        event for event in events if event.event_name == "harness.source.closed"
    ]
    assert len(closures) == 1
    payload = cast(dict[str, Any], closures[0].payload)
    assert payload["sequence"] == (3 if ending == "exit" else 2)
