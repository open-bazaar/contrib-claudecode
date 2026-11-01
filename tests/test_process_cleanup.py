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

GROUP = "{'creationflags':subprocess.CREATE_NEW_PROCESS_GROUP} if os.name=='nt' else {}"
SESSION = (
    "{'creationflags':subprocess.CREATE_NEW_PROCESS_GROUP} if os.name=='nt'"
    " else {'start_new_session':True}"
)
CASES = [
    (descendant, ending)
    for descendant in ("child", "grandchild", "detached")
    for ending in ("exit", "stop", "zero", "cancel")
    # Once the root exits, a descendant in its own session is neither in the
    # root's group nor its tree; only a Windows Job still owns it.
    if sys.platform == "win32" or (descendant, ending) != ("detached", "exit")
]


def script(descendant: str) -> str:
    leaf = (
        "import os,sys,threading;"
        "print('leaf stderr',file=sys.stderr,flush=True);"
        "print('leaf:'+str(os.getpid()),flush=True);"
        "threading.Event().wait()"
    )
    if descendant == "detached":
        start = (
            "child=subprocess.Popen([sys.executable,'-c',"
            "'import threading;threading.Event().wait()'],"
            "stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,"
            f"stderr=subprocess.DEVNULL,**({SESSION}));"
            "print('leaf:'+str(child.pid),flush=True);"
        )
    elif descendant == "grandchild":
        orphan = (
            "import os,subprocess,sys;"
            f"subprocess.Popen([sys.executable,'-c',{leaf!r}],"
            f"stdin=subprocess.DEVNULL,**({GROUP}))"
        )
        start = (
            f"child=subprocess.Popen([sys.executable,'-c',{orphan!r}],"
            f"stdin=subprocess.DEVNULL,**({GROUP}));"
            "assert child.wait()==0;"
        )
    else:
        start = (
            f"child=subprocess.Popen([sys.executable,'-c',{leaf!r}],"
            f"stdin=subprocess.DEVNULL,**({GROUP}));"
        )
    return (
        "import os,subprocess,sys;" + start + "print('root-ready',flush=True);"
        "sys.exit(7 if sys.stdin.readline() else 0)"
    )


@pytest.mark.parametrize(("descendant", "ending"), CASES)
async def test_stop_cleans_descendants(tmp_path: Path, descendant: str, ending: str):
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
        supervisor = ProcessSupervisor(
            ProcessSpec(
                sys.executable, ("-c", script(descendant)), tmp_path, dict(os.environ)
            ),
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
