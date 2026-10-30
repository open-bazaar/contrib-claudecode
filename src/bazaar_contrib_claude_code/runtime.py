# Existing CLI and public Harness call budgets are explicit.
# ruff: noqa: ASYNC109

from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
from dataclasses import dataclass, field
from importlib.metadata import version
from pathlib import Path
from typing import Any
from uuid import uuid4

from bazaar_contrib_audit import Audit, AuditContext
from bazaar_contrib_harness import (
    ApprovalHandler,
    HarnessExpire,
    HarnessFailure,
    HarnessFailureReason,
    HarnessInput,
    HarnessSession,
    HarnessSessionReconciliation,
    HarnessSessionUnavailable,
    HarnessTurn,
    HarnessTurnStream,
    ProviderCallResult,
    ProviderCallStatus,
    SandboxPolicy,
    SessionInput,
)
from bazaar_contrib_timer import Timer
from watchfiles import awatch

from .approval import (
    build_approval_response,
    parse_approval_request,
)
from .children import Children
from .client import Client, TurnInbox
from .environment import environment
from .events import TurnEventStream, TurnState
from .process import ProcessSpec, ProcessSupervisor, build_arguments
from .protocol import (
    ClaudeControlError,
    ClaudeProtocolError,
    ClaudeTransportError,
    failure,
)
from .source import SourceAudit
from .timing import timeout as deadline_timeout

_MINIMUM_VERSION = (2, 1, 239)
_VERSION_PATTERN = re.compile(r"(?<!\d)(\d+)\.(\d+)\.(\d+)(?!\d)")
_UNUSABLE_CONNECTION_STOP_SECONDS = 10.0


@dataclass(slots=True)
class _Connection:
    supervisor: ProcessSupervisor
    client: Client
    workspace: Path
    provider_thread_id: str
    claude_version: tuple[int, int, int]
    state_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    active_turn_id: str | None = None
    active_stream: TurnEventStream | None = None
    active_background_task_ids: set[str] = field(default_factory=set)
    watch_task: asyncio.Task[None] | None = None


class ClaudeCodeHarness:
    """Run one persistent external Claude Code process per runtime session."""

    def __init__(
        self,
        input: HarnessInput,
        timer: Timer,
        audit: Audit,
        model: str | None = None,
        effort: str | None = None,
    ) -> None:
        self._input = input
        self._timer = timer
        self._audit = audit
        self._children = Children(timer)
        self._sessions: dict[str, SessionInput] = {}
        self._model = model
        self._effort = effort
        self._connections: dict[str, _Connection] = {}
        self._lifecycle_events: asyncio.Queue[HarnessExpire] = asyncio.Queue()
        self._retirement_tasks: set[asyncio.Task[None]] = set()
        self._started = True
        self._stopping = False

    async def aclose(self) -> None:
        try:
            await self._stop(timeout=self._input.shutdown_timeout)
        finally:
            await self._children.end_all()

    async def _stop(self, *, timeout: float) -> None:
        if self._stopping:
            return
        self._stopping = True
        retirement_tasks = tuple(self._retirement_tasks)
        if retirement_tasks:
            await asyncio.gather(*retirement_tasks, return_exceptions=True)
        connections = tuple(self._connections.values())
        self._connections.clear()
        for connection in connections:
            try:
                await self._stop_connection(connection, timeout=timeout)
            except OSError, TimeoutError, ClaudeTransportError:
                continue
        self._started = False

    async def receive_event(self) -> HarnessExpire:
        return await self._lifecycle_events.get()

    async def start_session(
        self, request: SessionInput, *, timeout: float
    ) -> ProviderCallResult[HarnessSession]:
        self._ensure_started()
        session = HarnessSession(id=request.id)
        self._sessions[request.id] = request
        existing = self._connections.pop(session.id, None)
        if existing is not None:
            await self._stop_connection(existing, timeout=timeout)
        provider_thread_id = str(uuid4())
        connection: _Connection | None = None
        try:
            connection = await self._open_connection(
                request,
                provider_thread_id=provider_thread_id,
                resume=False,
                timeout=timeout,
            )
            self._connections[session.id] = connection
            updated = session.model_copy(
                update={"provider_thread_id": provider_thread_id}
            )
            return ProviderCallResult(
                status=ProviderCallStatus.CONFIRMED,
                value=updated,
                receipt={"provider_thread_id": provider_thread_id},
            )
        except asyncio.CancelledError:
            if connection is not None:
                await self._stop_connection(connection, timeout=timeout)
            raise
        except Exception as error:  # noqa: BLE001
            if connection is not None:
                await self._stop_connection(connection, timeout=timeout)
            return _provider_result(error)

    async def reconcile_session(
        self,
        session: HarnessSession,
        turn: HarnessTurn | None,
        approval_handler: ApprovalHandler | None,
        *,
        timeout: float,
    ) -> ProviderCallResult[HarnessSessionReconciliation]:
        del turn, approval_handler
        self._ensure_started()
        provider_thread_id = session.provider_thread_id
        if provider_thread_id is None:
            return ProviderCallResult(
                status=ProviderCallStatus.FAILED,
                failure=HarnessFailure(
                    HarnessFailureReason.STATE_MISMATCH,
                    "cannot reconcile without a provider thread",
                ),
            )
        request = self._sessions.get(session.id)
        if request is None:
            return ProviderCallResult(
                status=ProviderCallStatus.FAILED,
                failure=HarnessFailure(
                    HarnessFailureReason.UNAVAILABLE,
                    "No session input owns this Claude Code session",
                ),
            )
        existing = self._connections.pop(session.id, None)
        if existing is not None:
            await self._stop_connection(existing, timeout=timeout)
        connection: _Connection | None = None
        try:
            connection = await self._open_connection(
                request,
                provider_thread_id=provider_thread_id,
                resume=True,
                timeout=timeout,
            )
            self._connections[session.id] = connection
            return ProviderCallResult(
                status=ProviderCallStatus.CONFIRMED,
                value=HarnessSessionReconciliation(
                    session=session,
                ),
                receipt={"provider_thread_id": provider_thread_id},
            )
        except asyncio.CancelledError:
            if connection is not None:
                await self._stop_connection(connection, timeout=timeout)
            raise
        except Exception as error:  # noqa: BLE001
            if connection is not None:
                await self._stop_connection(connection, timeout=timeout)
            return _provider_result(error)

    async def stop_session(
        self, session: HarnessSession, *, timeout: float
    ) -> ProviderCallResult[HarnessSession]:
        self._sessions.pop(session.id, None)
        connection = self._connections.pop(session.id, None)
        if connection is not None:
            await self._stop_connection(connection, timeout=timeout)
        return ProviderCallResult(status=ProviderCallStatus.CONFIRMED, value=session)

    async def _open_turn(
        self,
        connection: _Connection,
        session: HarnessSession,
        turn: HarnessTurn,
        input_text: str,
        approval_handler: ApprovalHandler,
        *,
        timeout: float,
    ) -> tuple[TurnInbox, BaseException | None]:
        """Claim the connection for this turn and ask the provider to start it."""

        async with connection.state_lock:
            if connection.active_turn_id is not None:
                raise RuntimeError(
                    f"runtime session already has an active turn: "
                    f"{connection.active_turn_id}"
                )
            connection.active_turn_id = turn.id
            if connection.supervisor.source is not None:
                connection.supervisor.source.turn_id = turn.id

            async def handle_permission(
                message: dict[str, object],
            ) -> dict[str, object]:
                approval = parse_approval_request(
                    message,
                    session_id=session.id,
                    turn_id=turn.id,
                )
                result = await approval_handler.request_approval(
                    approval.request, timeout=timeout
                )
                return build_approval_response(approval, result)

            connection.client.set_control_request_handler(handle_permission)
            try:
                (
                    inbox,
                    initial_error,
                ) = await connection.client.open_turn(input_text, timeout=timeout)
            except asyncio.CancelledError:
                connection.active_turn_id = None
                await connection.client.clear_control_request_handler(handle_permission)
                raise
            if initial_error is not None:
                await connection.client.clear_control_request_handler(handle_permission)
        return inbox, initial_error

    async def start_turn(
        self,
        session: HarnessSession,
        turn: HarnessTurn,
        approval_handler: ApprovalHandler,
        *,
        timeout: float,
    ) -> HarnessTurnStream:
        self._ensure_started()
        connection = self._connections.get(session.id)
        if connection is None or not connection.supervisor.is_running:
            raise HarnessSessionUnavailable("Claude Code process is not running")
        inbox, initial_error = await self._open_turn(
            connection,
            session,
            turn,
            turn.input_text,
            approval_handler,
            timeout=timeout,
        )
        if isinstance(initial_error, ClaudeControlError):
            initial_error_state, initial_error_kind = (
                TurnState.FAILED,
                "provider_failed",
            )
        elif isinstance(initial_error, ClaudeProtocolError):
            initial_error_state, initial_error_kind = (
                TurnState.FAILED,
                "protocol",
            )
        else:
            initial_error_state, initial_error_kind = (
                TurnState.UNKNOWN,
                "provider_unknown",
            )

        async def close_turn() -> None:
            await connection.client.close_turn(inbox)
            async with connection.state_lock:
                if connection.active_turn_id == turn.id:
                    connection.active_turn_id = None
                    if connection.supervisor.source is not None:
                        connection.supervisor.source.turn_id = None
                    connection.active_stream = None

        async def retire_connection(error: BaseException) -> None:
            del error
            await self._retire_connection(session.id, connection)

        stream = TurnEventStream(
            inbox,
            session_id=session.id,
            turn_id=turn.id,
            provider_thread_id=connection.provider_thread_id,
            claude_version=connection.claude_version,
            on_closed=close_turn,
            on_unusable=retire_connection,
            initial_error=initial_error,
            initial_error_state=initial_error_state,
            initial_error_kind=initial_error_kind,
        )
        async with connection.state_lock:
            connection.active_stream = stream
        return stream

    async def steer_turn(
        self,
        session: HarnessSession,
        turn: HarnessTurn,
        input_text: str,
        *,
        timeout: float,
    ) -> bool:
        self._ensure_started()
        connection = self._connections.get(session.id)
        if connection is None or not connection.supervisor.is_running:
            return False
        async with connection.state_lock:
            if connection.active_turn_id != turn.id:
                return False
            try:
                return await connection.client.send_user_message(
                    input_text, timeout=timeout
                )
            except ClaudeTransportError:
                return False

    async def has_background_job(
        self, session: HarnessSession, *, timeout: float
    ) -> bool:
        del timeout
        connection = self._connections.get(session.id)
        if connection is None:
            return False
        async with connection.state_lock:
            return bool(
                connection.active_background_task_ids
                or connection.client.provider_wake_active
            )

    async def _open_connection(
        self,
        session: SessionInput,
        *,
        provider_thread_id: str,
        resume: bool,
        timeout: float,
    ) -> _Connection:
        executable = await asyncio.to_thread(shutil.which, "claude")
        if executable is None:
            raise FileNotFoundError("Claude executable not found: claude")
        workspace = session.workspace
        await asyncio.to_thread(
            workspace.mkdir, parents=True, exist_ok=True, mode=0o700
        )
        if os.name != "nt":
            await asyncio.to_thread(workspace.chmod, 0o700)
        process_environment = environment() | session.environment
        process_environment["CLAUDE_AGENT_SDK_CLIENT_APP"] = (
            f"bazaar-contrib-claude-code/{version('bazaar-contrib-claude-code')}"
        )
        deadline = asyncio.get_running_loop().time() + min(
            timeout, self._input.startup_timeout
        )
        claude_version = await _check_version(
            executable,
            workspace,
            process_environment,
            timer=self._timer,
            timeout=_remaining(deadline),
        )
        prompt = "\n\n".join(session.instructions)
        settings = json.dumps(
            _sandbox_settings(session.sandbox_policy), separators=(",", ":")
        )
        arguments = build_arguments(
            system_prompt=prompt,
            settings=settings,
            permission_mode=(
                "bypassPermissions"
                if session.sandbox_policy is SandboxPolicy.DANGER_FULL_ACCESS
                else "default"
            ),
            session_id=None if resume else provider_thread_id,
            resume=provider_thread_id if resume else None,
            model=self._model,
            effort=self._effort,
        )
        supervisor = ProcessSupervisor(
            ProcessSpec(
                executable=executable,
                arguments=arguments,
                cwd=workspace,
                environment=process_environment,
            ),
            self._children,
            source=SourceAudit(
                self._audit,
                AuditContext(
                    agent_id=self._input.agent_id,
                    harness="claudecode",
                    session_id=session.id,
                    provider_session_id=provider_thread_id,
                ),
            ),
        )
        client = Client(supervisor)
        try:
            await supervisor.start(timeout=_remaining(deadline))
            await client.initialize(timeout=_remaining(deadline))
        except BaseException:
            await supervisor.stop(timeout=timeout)
            await client.close()
            raise
        connection = _Connection(
            supervisor,
            client,
            workspace,
            provider_thread_id,
            claude_version,
        )

        def observe(message: dict[str, object]) -> None:
            if (
                message["type"] == "conversation_reset"
                and not client.has_foreground_turn
                and not self._stopping
            ):
                task = asyncio.create_task(
                    self._retire_connection(session.id, connection),
                    name=f"claude-code-retire-{session.id}",
                )
                self._retirement_tasks.add(task)
                task.add_done_callback(self._retirement_tasks.discard)
                return
            _observe_background(connection, message)

        client.set_message_observer(observe)
        home = await asyncio.to_thread(
            lambda: Path(
                process_environment.get(
                    "CLAUDE_CONFIG_DIR", str(Path.home() / ".claude")
                )
            ).expanduser()
        )
        if not home.is_absolute():
            home = workspace / home
        paths = []
        for root in (workspace, home):
            for name in ("AGENTS.md", "CLAUDE.md"):
                path = root / name
                if await asyncio.to_thread(path.is_file):
                    paths.append(path)
        if paths:
            connection.watch_task = asyncio.create_task(
                self._watch_files(session.id, tuple(paths)),
                name=f"claude-code-watch-{session.id}",
            )
        return connection

    async def _watch_files(self, session_id: str, paths: tuple[Path, ...]) -> None:
        async for _ in awatch(*paths):
            self._lifecycle_events.put_nowait(HarnessExpire(session_id=session_id))

    async def _stop_connection(
        self, connection: _Connection, *, timeout: float
    ) -> None:
        if connection.watch_task is not None:
            connection.watch_task.cancel()
        try:
            await connection.supervisor.stop(timeout=timeout)
        finally:
            try:
                await connection.client.close()
            finally:
                if connection.watch_task is not None:
                    await asyncio.gather(connection.watch_task, return_exceptions=True)

    async def _retire_connection(
        self, session_id: str, connection: _Connection
    ) -> None:
        if self._connections.get(session_id) is not connection:
            return
        self._connections.pop(session_id, None)
        try:
            await self._stop_connection(
                connection, timeout=_UNUSABLE_CONNECTION_STOP_SECONDS
            )
        except OSError, TimeoutError, ClaudeTransportError:
            return

    def _ensure_started(self) -> None:
        if not self._started or self._stopping:
            raise RuntimeError("Claude Code runtime is not started")


async def _check_version(
    executable: str,
    cwd: Path,
    environment: dict[str, str],
    *,
    timeout: float,
    timer: Timer | None = None,
) -> tuple[int, int, int]:
    process: asyncio.subprocess.Process | None = None
    try:
        async with deadline_timeout(timer, timeout):
            process = await asyncio.create_subprocess_exec(
                executable,
                "--version",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(cwd),
                env=environment,
            )
            stdout, stderr = await process.communicate()
    except BaseException:
        if process is not None and process.returncode is None:
            process.kill()
            await process.wait()
        raise
    assert process is not None
    text = stdout.decode(errors="replace").strip()
    match = _VERSION_PATTERN.search(text)
    if process.returncode != 0 or match is None:
        detail = stderr.decode(errors="replace").strip() or text
        raise ClaudeProtocolError(f"cannot determine Claude Code version: {detail}")
    version = (int(match[1]), int(match[2]), int(match[3]))
    if version < _MINIMUM_VERSION:
        raise ClaudeProtocolError(
            f"Claude Code {text} is below required version 2.1.239"
        )
    return version


def _sandbox_settings(mode: SandboxPolicy) -> dict[str, object]:
    if mode is SandboxPolicy.DANGER_FULL_ACCESS:
        return {"sandbox": {"enabled": False}}
    return {
        "sandbox": {
            "enabled": True,
            "failIfUnavailable": True,
            "autoAllowBashIfSandboxed": True,
            "allowUnsandboxedCommands": True,
            "network": {
                "allowedDomains": ["*"],
                "allowAllUnixSockets": True,
                "allowLocalBinding": True,
            },
        }
    }


def _observe_background(connection: _Connection, message: dict[str, object]) -> None:
    # Claude Code republishes the whole set on every change, so the latest
    # snapshot is the answer and nothing has to be accumulated from edges
    if (
        message.get("type") != "system"
        or message.get("subtype") != "background_tasks_changed"
    ):
        return
    tasks = message.get("tasks")
    if not isinstance(tasks, list):
        return
    connection.active_background_task_ids = {
        task_id
        for task in tasks
        if isinstance(task, dict)
        and isinstance(task_id := task.get("task_id"), str)
        and task_id
    }


def _provider_result(error: BaseException) -> ProviderCallResult[Any]:
    return ProviderCallResult(status=ProviderCallStatus.FAILED, failure=failure(error))


def _remaining(deadline: float) -> float:
    remaining = deadline - asyncio.get_running_loop().time()
    if remaining <= 0:
        raise TimeoutError
    return remaining
