"""The runtime processes this provider started and that have not ended: when
going down runs out of time, the node ends them itself before it exits, so
none is left running without it. Each has its own group or Windows Job, so
what it starts in turn ends with it."""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
from collections.abc import Sequence
from typing import Any

import psutil
from bazaar_contrib_timer import Timer

if sys.platform == "win32":
    from .windows_job import CREATE_SUSPENDED, WindowsJob

# how long a runtime asked to end gets before it is killed
END_GRACE_SECONDS = 2.0


def own_group() -> dict[str, Any]:
    """What `create_subprocess_exec` takes to start a process in a group of
    its own: the terminal's Ctrl-C does not reach it, the node ends it.
    On Windows, Children.started assigns the suspended process before resuming."""

    if sys.platform == "win32":
        return {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP | CREATE_SUSPENDED}
    return {"start_new_session": True}


class Children:
    def __init__(self, timer: Timer | None = None):
        self.timer = timer
        self._running: set[int] = set()
        self._reapers: set[asyncio.Task[None]] = set()
        if sys.platform == "win32":
            self._jobs: dict[int, WindowsJob] = {}

    def started(self, pid: int) -> None:
        self._running.add(pid)
        if sys.platform == "win32":
            # Own the entire family before the CLI can create any descendants.
            job = WindowsJob()
            self._jobs[pid] = job
            job.assign(pid)
            psutil.Process(pid).resume()

    def reaping(self, task: asyncio.Task[None]) -> None:
        self._reapers.add(task)
        task.add_done_callback(self._reapers.discard)

    def snapshot(self, pid: int) -> tuple[psutil.Process, ...]:
        # Cancellation must not kill the root before its descendants are captured.
        try:
            process = psutil.Process(pid)
            descendants = process.children(recursive=True)
        except psutil.NoSuchProcess:
            return ()
        return (process, *descendants)

    async def ended(self, pid: int, tree: Sequence[psutil.Process] = ()) -> None:
        """A runtime is gone: what it started and left running goes with it, so
        nothing it began outlives it unregistered."""

        if sys.platform == "win32":
            if job := self._jobs.get(pid):
                job.close()
                del self._jobs[pid]
        else:
            await asyncio.to_thread(_signal_group, pid, signal.SIGKILL)
        await asyncio.to_thread(_kill_tree, tree)
        self._running.discard(pid)

    async def wait(self, tree: Sequence[psutil.Process]) -> None:
        remaining = list(tree)
        while remaining:
            _, remaining = await asyncio.to_thread(
                psutil.wait_procs, remaining, timeout=0
            )
            if remaining:
                if self.timer is None:
                    await asyncio.sleep(0.05)
                else:
                    await self.timer.create(50).wait()

    async def end_all(self) -> None:
        """End every runtime still running, with what it started: asked first,
        killed when it is still there after the grace."""

        pids = tuple(self._running)
        if not pids:
            await asyncio.gather(*tuple(self._reapers), return_exceptions=True)
            return
        trees: dict[int, tuple[psutil.Process, ...]] = {}

        def snapshot() -> None:
            for pid in pids:
                trees[pid] = self.snapshot(pid)

        try:
            capture = asyncio.create_task(asyncio.to_thread(snapshot))
            try:
                await asyncio.shield(capture)
            except asyncio.CancelledError:
                await capture
                raise
            if sys.platform == "win32":
                return
            for pid in pids:
                await asyncio.to_thread(_signal_group, pid, signal.SIGTERM)
            loop = asyncio.get_running_loop()
            deadline = loop.time() + END_GRACE_SECONDS
            roots = [tree[0] for tree in trees.values() if tree]
            remaining = [process for tree in trees.values() for process in tree[1:]]
            while loop.time() < deadline:
                _, remaining = await asyncio.to_thread(
                    psutil.wait_procs, remaining, timeout=0
                )
                if not remaining and not await asyncio.to_thread(
                    lambda: any(root.is_running() for root in roots)
                ):
                    break
                if self.timer is None:
                    await asyncio.sleep(0.05)
                else:
                    await self.timer.create(50).wait()
        finally:
            for pid in pids:
                await self.ended(pid, trees.get(pid, ()))
            await asyncio.gather(*tuple(self._reapers), return_exceptions=True)


def _kill_tree(tree: Sequence[psutil.Process]) -> None:
    for process in reversed(tree):
        try:
            process.kill()
        except psutil.NoSuchProcess:
            pass


if sys.platform != "win32":

    def _signal_group(pid: int, number: signal.Signals) -> None:
        try:
            os.killpg(pid, number)
        except ProcessLookupError:
            pass
        except PermissionError:
            if sys.platform != "darwin" or _group_has_live_members(pid):
                raise

    def _group_has_live_members(pid: int) -> bool:
        # Darwin also returns EPERM for a group containing only zombies. Check
        # the kernel's process list before treating a permission error as exit.
        for process in psutil.process_iter(("pid", "status")):
            try:
                if (
                    process.info["status"] != psutil.STATUS_ZOMBIE
                    and os.getpgid(process.pid) == pid
                ):
                    return True
            except ProcessLookupError:
                pass
        return False
