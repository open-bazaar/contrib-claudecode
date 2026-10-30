# Existing CLI and public Harness call budgets are explicit.
# ruff: noqa: ASYNC109

from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import cast

from .children import Children, own_group
from .line_reader import UnlimitedLineReader
from .protocol import (
    ClaudeProcessExited,
    ClaudeProcessNotRunning,
    ClaudeProtocolError,
    ClaudeTransportError,
    JsonObject,
    validate_envelope,
)
from .source import SourceAudit
from .timing import timeout as deadline_timeout

MAX_JSONL_BYTES = 1024 * 1024
_EXIT_PIPE_DRAIN_SECONDS = 1
_CLOSED = object()
_REAP_REPORT_SECONDS = 30.0
_LOGGER = logging.getLogger("bazaar_contrib_claude_code.process")


class ProcessState(StrEnum):
    STOPPED = "stopped"
    RUNNING = "running"
    STOPPING = "stopping"
    EXITED = "exited"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class ProcessSpec:
    executable: str
    arguments: tuple[str, ...]
    cwd: Path
    environment: Mapping[str, str]

    @property
    def command(self) -> tuple[str, ...]:
        return (self.executable, *self.arguments)


def build_arguments(
    *,
    system_prompt: str,
    settings: str,
    permission_mode: str = "default",
    session_id: str | None = None,
    resume: str | None = None,
    model: str | None = None,
    effort: str | None = None,
) -> tuple[str, ...]:
    if (session_id is None) == (resume is None):
        raise ValueError("exactly one of session_id or resume is required")
    arguments = [
        "--output-format",
        "stream-json",
        "--verbose",
        "--append-system-prompt",
        system_prompt,
    ]
    if model is not None:
        arguments.extend(("--model", model))
    arguments.extend(
        (
            "--permission-prompt-tool",
            "stdio",
            "--permission-mode",
            permission_mode,
            "--disallowedTools",
            "AskUserQuestion",
            f"--session-id={session_id}"
            if session_id is not None
            else f"--resume={resume}",
            "--settings",
            settings,
            "--include-partial-messages",
        )
    )
    if effort is not None:
        arguments.extend(("--effort", effort))
    arguments.extend(("--input-format", "stream-json"))
    return tuple(arguments)


def decode_stdout_line(line: bytes) -> JsonObject | None:
    if len(line) + 1 > MAX_JSONL_BYTES:
        raise ClaudeProtocolError("stdout JSONL item exceeds 1 MiB")
    text = line.decode("utf-8", errors="strict").strip()
    if not text or not text.startswith("{"):
        return None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as error:
        raise ClaudeProtocolError("stdout contains invalid JSONL") from error
    if not isinstance(payload, dict):
        raise ClaudeProtocolError("stdout JSONL item must be an object")
    return validate_envelope(payload)


class ProcessSupervisor:
    """Own one long-lived Claude CLI process and its JSONL transport."""

    def __init__(
        self,
        spec: ProcessSpec,
        children: Children,
        *,
        source: SourceAudit | None = None,
        stderr_tail_limit: int = 64,
    ) -> None:
        self.spec = spec
        self.children = children
        self.timer = children.timer
        self.source = source
        self._process: asyncio.subprocess.Process | None = None
        self._state = ProcessState.STOPPED
        self._returncode: int | None = None
        self._fatal_error: ClaudeTransportError | None = None
        self._stderr_tail: deque[str] = deque(maxlen=stderr_tail_limit)
        self._result_error_tail: deque[str] = deque(maxlen=stderr_tail_limit)
        self._incoming: asyncio.Queue[JsonObject | object] = asyncio.Queue()
        self._stdout_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._watch_task: asyncio.Task[None] | None = None
        self._write_lock = asyncio.Lock()
        self._lifecycle_lock = asyncio.Lock()
        self._exit_event = asyncio.Event()
        self._started = False

    @property
    def state(self) -> ProcessState:
        return self._state

    @property
    def pid(self) -> int | None:
        return self._process.pid if self._process is not None else None

    @property
    def returncode(self) -> int | None:
        return self._returncode

    @property
    def is_running(self) -> bool:
        return self._process is not None and self._process.returncode is None

    @property
    def stderr_tail(self) -> tuple[str, ...]:
        return tuple(self._stderr_tail)

    @property
    def result_error_tail(self) -> tuple[str, ...]:
        return tuple(self._result_error_tail)

    async def start(self, *, timeout: float) -> None:
        async with self._lifecycle_lock:
            if self._started:
                raise RuntimeError("a stopped supervisor cannot start again")
            self._started = True
            self._state = ProcessState.RUNNING
            try:
                async with deadline_timeout(self.timer, timeout):
                    process = await asyncio.create_subprocess_exec(
                        *self.spec.command,
                        stdin=asyncio.subprocess.PIPE,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                        cwd=str(self.spec.cwd),
                        env=dict(self.spec.environment),
                        **own_group(),
                    )
            except BaseException:
                self._state = ProcessState.FAILED
                raise
            self._process = process
            self.children.started(process.pid)
            self._stdout_task = asyncio.create_task(
                self._read_stdout(process), name="claude-code-stdout"
            )
            self._stderr_task = asyncio.create_task(
                self._read_stderr(process), name="claude-code-stderr"
            )
            self._watch_task = asyncio.create_task(
                self._watch_process(process), name="claude-code-process"
            )

    async def send(self, envelope: Mapping[str, object]) -> None:
        message = validate_envelope(envelope)
        encoded = (
            json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode()
            + b"\n"
        )
        if len(encoded) > MAX_JSONL_BYTES:
            raise ClaudeProtocolError("outgoing JSONL item exceeds 1 MiB")
        async with self._write_lock:
            self._ensure_running()
            process = self._process
            if process is None or process.stdin is None:
                raise ClaudeProcessNotRunning
            try:
                process.stdin.write(encoded)
                if self.source is not None:
                    await self.source.publish(encoded.decode(), "sent")
                await process.stdin.drain()
            except (BrokenPipeError, ConnectionError, OSError) as error:
                raise ClaudeProcessExited(
                    process.returncode,
                    self.stderr_tail,
                    self.result_error_tail,
                ) from error

    async def receive(self) -> JsonObject:
        item = await self._incoming.get()
        if item is _CLOSED:
            if self._fatal_error is not None:
                raise self._fatal_error
            raise ClaudeProcessExited(
                self._returncode,
                self.stderr_tail,
                self.result_error_tail,
            )
        return cast(JsonObject, item)

    async def wait(self, *, timeout: float | None = None) -> int | None:
        if timeout is None:
            await self._exit_event.wait()
        else:
            async with deadline_timeout(self.timer, timeout):
                await self._exit_event.wait()
        return self._returncode

    async def stop(self, *, timeout: float) -> None:
        async with self._lifecycle_lock:
            process = self._process
            if process is None:
                self._state = ProcessState.STOPPED
                return
            self._state = ProcessState.STOPPING
            now = asyncio.get_running_loop().time()
            graceful_deadline = now + timeout * 0.6
            terminate_deadline = now + timeout * 0.9
            kill_deadline = now + timeout
            tree = ()
            try:
                tree = self.children.snapshot(process.pid)
                if process.stdin is not None:
                    process.stdin.close()
                for deadline, operation in (
                    (graceful_deadline, None),
                    (terminate_deadline, "terminate"),
                    (kill_deadline, "kill"),
                ):
                    if operation == "kill":
                        self.children.ended(process.pid, tree)
                    elif operation is not None and process.returncode is None:
                        try:
                            process.terminate()
                        except ProcessLookupError:
                            pass
                    try:
                        await self._wait_until(process, deadline)
                    except TimeoutError:
                        continue
                    break
                else:
                    raise TimeoutError
                self.children.ended(process.pid, tree)
                remaining = kill_deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise TimeoutError
                async with deadline_timeout(self.timer, remaining):
                    await self.children.wait(tree)
                self._returncode = process.returncode
                await self._join_tasks(cancel=True)
                if self.source is not None:
                    await self.source.close(self._returncode)
            except (TimeoutError, asyncio.CancelledError) as error:
                self._abandon(process)
                self._state = ProcessState.STOPPED
                self._exit_event.set()
                if isinstance(error, asyncio.CancelledError):
                    raise
                return
            finally:
                if process.returncode is None:
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                self.children.ended(process.pid, tree)
            self._state = ProcessState.STOPPED
            self._exit_event.set()

    def _abandon(self, process: asyncio.subprocess.Process) -> None:
        """Give up on a process without losing track of it.

        Closing this side of the pipes is what unblocks everything: the readers
        stop waiting for an EOF that a grandchild holding the write end will
        never deliver, and the exit waiters can complete once the pipes count as
        disconnected. Whoever still holds that write end is free to keep it.
        """

        transport = getattr(process, "_transport", None)
        if transport is not None:
            for fd in (1, 2):
                pipe = transport.get_pipe_transport(fd)
                if pipe is not None:
                    pipe.close()
        # Release receive() before the watcher finishes its owned cleanup.
        self._incoming.put_nowait(_CLOSED)
        tasks = tuple(
            task
            for task in (self._stdout_task, self._stderr_task, self._watch_task)
            if task is not None and task is not asyncio.current_task()
        )
        for task in tasks:
            if task is not self._watch_task:
                task.cancel()
        self._stdout_task = None
        self._stderr_task = None
        self._watch_task = None
        self._process = None
        reaper = asyncio.create_task(
            self._reap(process, tasks),
            name=f"bcn-claude-reap-{process.pid}",
        )
        self.children.reaping(reaper)

    async def _reap(
        self,
        process: asyncio.subprocess.Process,
        tasks: tuple[asyncio.Task[None], ...],
    ) -> None:
        """Outlive the caller waiting for a process nobody else is waiting for."""

        try:
            async with deadline_timeout(self.timer, _REAP_REPORT_SECONDS):
                returncode = await process.wait()
        except TimeoutError:
            _LOGGER.warning("process %s outlived its kill; still waiting", process.pid)
            returncode = await process.wait()
        self._returncode = returncode
        self.children.ended(process.pid)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self.source is not None:
            await self.source.close(returncode)

    async def _read_stdout(self, process: asyncio.subprocess.Process) -> None:
        stdout = process.stdout
        if stdout is None:
            await self._fail(ClaudeProtocolError("stdout pipe is unavailable"))
            return
        reader = UnlimitedLineReader(stdout)
        try:
            while line := await reader.readline():
                if self.source is not None:
                    await self.source.publish(line.decode(errors="replace"), "received")
                payload = decode_stdout_line(line.removesuffix(b"\n"))
                if payload is None:
                    continue
                if payload["type"] == "result":
                    self._result_error_tail.clear()
                    errors = payload.get("errors")
                    if isinstance(errors, list):
                        for error in errors:
                            if isinstance(error, str) and error.strip():
                                self._result_error_tail.append(error.strip())
                await self._incoming.put(payload)
        except (UnicodeDecodeError, json.JSONDecodeError, ClaudeProtocolError) as error:
            await self._fail(ClaudeProtocolError(str(error)))
        except (ConnectionError, OSError) as error:
            await self._fail(ClaudeTransportError(type(error).__name__))

    async def _read_stderr(self, process: asyncio.subprocess.Process) -> None:
        stderr = process.stderr
        if stderr is None:
            return
        reader = UnlimitedLineReader(stderr)
        try:
            while line := await reader.readline():
                self._stderr_tail.append(
                    line.decode("utf-8", errors="replace").rstrip("\r\n")
                )
        except ConnectionError, OSError:
            return

    async def _watch_process(self, process: asyncio.subprocess.Process) -> None:
        self._returncode = await process.wait()
        self.children.ended(process.pid)
        readers = tuple(
            task for task in (self._stdout_task, self._stderr_task) if task is not None
        )
        if readers:
            try:
                async with deadline_timeout(self.timer, _EXIT_PIPE_DRAIN_SECONDS):
                    await asyncio.gather(*readers, return_exceptions=True)
            except TimeoutError:
                for task in readers:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*readers, return_exceptions=True)
        if self._returncode and self._fatal_error is None:
            self._fatal_error = ClaudeProcessExited(
                self._returncode,
                self.stderr_tail,
                self.result_error_tail,
            )
        self._state = (
            ProcessState.FAILED
            if self._fatal_error is not None
            else ProcessState.EXITED
        )
        self._exit_event.set()
        if self.source is not None:
            await self.source.close(self._returncode)
        await self._put_closed()

    async def _fail(self, error: ClaudeTransportError) -> None:
        if self._fatal_error is None:
            self._fatal_error = error
        process = self._process
        if process is not None and process.returncode is None:
            process.terminate()

    async def _put_closed(self) -> None:
        await self._incoming.put(_CLOSED)

    async def _wait_until(
        self, process: asyncio.subprocess.Process, deadline: float
    ) -> None:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise TimeoutError
        async with deadline_timeout(self.timer, remaining):
            await asyncio.shield(process.wait())

    async def _join_tasks(self, *, cancel: bool = False) -> None:
        tasks = tuple(
            task
            for task in (self._stdout_task, self._stderr_task, self._watch_task)
            if task is not None and task is not asyncio.current_task()
        )
        if tasks:
            if cancel:
                for task in tasks:
                    if task is not self._watch_task and not task.done():
                        task.cancel()
            await asyncio.shield(asyncio.gather(*tasks, return_exceptions=True))
        self._stdout_task = None
        self._stderr_task = None
        self._watch_task = None

    def _ensure_running(self) -> None:
        if not self.is_running:
            raise ClaudeProcessNotRunning


__all__ = [
    "MAX_JSONL_BYTES",
    "ProcessSpec",
    "ProcessState",
    "ProcessSupervisor",
    "build_arguments",
    "decode_stdout_line",
]
