# Existing CLI and public Harness call budgets are explicit.
# ruff: noqa: ASYNC109

from __future__ import annotations

import asyncio
import logging
import secrets
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import cast
from uuid import uuid4

from .process import ProcessSupervisor
from .protocol import (
    ClaudeProcessExited,
    ClaudeProtocolError,
    ClaudeTransportError,
    JsonObject,
    parse_control_response,
)
from .timing import timeout as deadline_timeout

_LOGGER = logging.getLogger(__name__)


class ProviderCycleState(Enum):
    """Claude's observable provider-cycle state."""

    IDLE = auto()
    FOREGROUND = auto()
    PROVIDER_WAKE = auto()
    PROVIDER_WAKE_ADOPTED = auto()


class _ProviderCycleEvent(Enum):
    FOREGROUND_OPENED = auto()
    FOREGROUND_CLOSED = auto()
    PROVIDER_WAKE_STARTED = auto()
    PROVIDER_WAKE_FINISHED = auto()


_PROVIDER_CYCLE_TRANSITIONS: dict[
    tuple[ProviderCycleState, _ProviderCycleEvent], ProviderCycleState
] = {
    (ProviderCycleState.IDLE, _ProviderCycleEvent.FOREGROUND_OPENED): (
        ProviderCycleState.FOREGROUND
    ),
    (ProviderCycleState.IDLE, _ProviderCycleEvent.FOREGROUND_CLOSED): (
        ProviderCycleState.IDLE
    ),
    (ProviderCycleState.IDLE, _ProviderCycleEvent.PROVIDER_WAKE_STARTED): (
        ProviderCycleState.PROVIDER_WAKE
    ),
    (ProviderCycleState.IDLE, _ProviderCycleEvent.PROVIDER_WAKE_FINISHED): (
        ProviderCycleState.IDLE
    ),
    (ProviderCycleState.FOREGROUND, _ProviderCycleEvent.FOREGROUND_CLOSED): (
        ProviderCycleState.IDLE
    ),
    (ProviderCycleState.FOREGROUND, _ProviderCycleEvent.PROVIDER_WAKE_STARTED): (
        ProviderCycleState.PROVIDER_WAKE_ADOPTED
    ),
    (ProviderCycleState.FOREGROUND, _ProviderCycleEvent.PROVIDER_WAKE_FINISHED): (
        ProviderCycleState.FOREGROUND
    ),
    (ProviderCycleState.PROVIDER_WAKE, _ProviderCycleEvent.FOREGROUND_OPENED): (
        ProviderCycleState.PROVIDER_WAKE_ADOPTED
    ),
    (ProviderCycleState.PROVIDER_WAKE, _ProviderCycleEvent.FOREGROUND_CLOSED): (
        ProviderCycleState.PROVIDER_WAKE
    ),
    (ProviderCycleState.PROVIDER_WAKE, _ProviderCycleEvent.PROVIDER_WAKE_STARTED): (
        ProviderCycleState.PROVIDER_WAKE
    ),
    (ProviderCycleState.PROVIDER_WAKE, _ProviderCycleEvent.PROVIDER_WAKE_FINISHED): (
        ProviderCycleState.IDLE
    ),
    (
        ProviderCycleState.PROVIDER_WAKE_ADOPTED,
        _ProviderCycleEvent.FOREGROUND_CLOSED,
    ): ProviderCycleState.PROVIDER_WAKE,
    (
        ProviderCycleState.PROVIDER_WAKE_ADOPTED,
        _ProviderCycleEvent.PROVIDER_WAKE_STARTED,
    ): ProviderCycleState.PROVIDER_WAKE_ADOPTED,
    (
        ProviderCycleState.PROVIDER_WAKE_ADOPTED,
        _ProviderCycleEvent.PROVIDER_WAKE_FINISHED,
    ): ProviderCycleState.FOREGROUND,
}


@dataclass(slots=True)
class ProviderCycleStateMachine:
    """Centralize foreground/provider-wake ownership transitions."""

    state: ProviderCycleState = ProviderCycleState.IDLE

    @property
    def provider_wake_active(self) -> bool:
        return self.state in {
            ProviderCycleState.PROVIDER_WAKE,
            ProviderCycleState.PROVIDER_WAKE_ADOPTED,
        }

    @property
    def provider_wake_adopted(self) -> bool:
        return self.state is ProviderCycleState.PROVIDER_WAKE_ADOPTED

    def foreground_opened(self) -> bool:
        self._transition(_ProviderCycleEvent.FOREGROUND_OPENED)
        return self.provider_wake_adopted

    def foreground_closed(self) -> None:
        self._transition(_ProviderCycleEvent.FOREGROUND_CLOSED)

    def provider_wake_started(self) -> None:
        self._transition(_ProviderCycleEvent.PROVIDER_WAKE_STARTED)

    def provider_wake_finished(self) -> None:
        self._transition(_ProviderCycleEvent.PROVIDER_WAKE_FINISHED)

    def _transition(self, event: _ProviderCycleEvent) -> None:
        try:
            self.state = _PROVIDER_CYCLE_TRANSITIONS[self.state, event]
        except KeyError as error:
            raise RuntimeError(
                f"invalid Claude provider-cycle transition: "
                f"{self.state.name} + {event.name}"
            ) from error


@dataclass(eq=False, slots=True)
class TurnInbox:
    """One BCN foreground turn's view of the connection message stream."""

    minimum_sequence: int
    adopted_provider_wake: bool = False
    pending_human_messages: set[str] = field(default_factory=set)
    accepting_input: bool = True
    result: JsonObject | None = None
    _messages: asyncio.Queue[JsonObject | BaseException] = field(
        default_factory=asyncio.Queue
    )

    async def receive(self) -> JsonObject:
        item = await self._messages.get()
        if isinstance(item, BaseException):
            raise item
        return item


class Client:
    """Correlate controls and route persistent-session provider turns."""

    def __init__(self, supervisor: ProcessSupervisor) -> None:
        self._supervisor = supervisor
        self._pending: dict[str, asyncio.Future[JsonObject]] = {}
        self._inflight_requests: dict[str, asyncio.Task[None]] = {}
        self._reader_task: asyncio.Task[None] | None = None
        self._route_lock = asyncio.Lock()
        self._turn_inbox: TurnInbox | None = None
        self._message_sequence = 0
        self._provider_cycle = ProviderCycleStateMachine()
        self._message_observer: Callable[[JsonObject], None] | None = None
        self._control_request_handler: (
            Callable[[JsonObject], Awaitable[Mapping[str, object]]] | None
        ) = None
        self._request_counter = 0

    @property
    def pending_control_count(self) -> int:
        return len(self._pending)

    @property
    def provider_wake_active(self) -> bool:
        return self._provider_cycle.provider_wake_active

    @property
    def provider_cycle_state(self) -> ProviderCycleState:
        return self._provider_cycle.state

    @property
    def has_foreground_turn(self) -> bool:
        return self._turn_inbox is not None

    def start(self) -> None:
        if self._reader_task is None:
            self._reader_task = asyncio.create_task(
                self._read_messages(), name="claude-code-client"
            )

    def set_message_observer(
        self, observer: Callable[[JsonObject], None] | None
    ) -> None:
        self._message_observer = observer

    def set_control_request_handler(
        self,
        handler: Callable[[JsonObject], Awaitable[Mapping[str, object]]] | None,
    ) -> None:
        self._control_request_handler = handler

    async def clear_control_request_handler(
        self,
        handler: Callable[[JsonObject], Awaitable[Mapping[str, object]]],
    ) -> None:
        if self._control_request_handler is not handler:
            return
        self._control_request_handler = None
        tasks = tuple(self._inflight_requests.values())
        self._inflight_requests.clear()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def initialize(self, *, timeout: float) -> JsonObject:
        return await self.control(
            {"subtype": "initialize", "hooks": None}, timeout=timeout
        )

    async def control(
        self, request: Mapping[str, object], *, timeout: float
    ) -> JsonObject:
        subtype = request.get("subtype")
        if not isinstance(subtype, str) or not subtype:
            raise ClaudeProtocolError("control request requires a subtype")
        self.start()
        request_id = self._next_request_id()
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        try:
            async with deadline_timeout(self._supervisor.timer, timeout):
                await self._supervisor.send(
                    {
                        "type": "control_request",
                        "request_id": request_id,
                        "request": dict(request),
                    }
                )
                response = await asyncio.shield(future)
            return parse_control_response(response)
        finally:
            if self._pending.get(request_id) is future:
                self._pending.pop(request_id, None)
            if not future.done():
                future.cancel()

    async def open_turn(
        self, text: str, *, timeout: float
    ) -> tuple[TurnInbox, BaseException | None]:
        """Attach a foreground inbox and submit its first human input atomically."""
        self.start()
        inbox = TurnInbox(self._message_sequence + 1)
        try:
            async with (
                deadline_timeout(self._supervisor.timer, timeout),
                self._route_lock,
            ):
                if self._turn_inbox is not None:
                    raise RuntimeError("Claude client already has a foreground turn")
                inbox.minimum_sequence = self._message_sequence + 1
                self._turn_inbox = inbox
                inbox.adopted_provider_wake = self._provider_cycle.foreground_opened()
                message_id = str(uuid4())
                await self._supervisor.send(_user_envelope(text, message_id))
                inbox.pending_human_messages.add(message_id)
        except asyncio.CancelledError:
            await self.close_turn(inbox)
            raise
        except Exception as error:  # noqa: BLE001
            return inbox, error
        return inbox, None

    async def close_turn(self, inbox: TurnInbox) -> None:
        async with self._route_lock:
            if self._turn_inbox is inbox:
                self._turn_inbox = None
                self._provider_cycle.foreground_closed()

    async def send_user_message(self, text: str, *, timeout: float) -> bool:
        async with (
            deadline_timeout(self._supervisor.timer, timeout),
            self._route_lock,
        ):
            inbox = self._turn_inbox
            if inbox is None or not inbox.accepting_input:
                return False
            message_id = str(uuid4())
            await self._supervisor.send(_user_envelope(text, message_id))
            inbox.pending_human_messages.add(message_id)
            return True

    async def close(self) -> None:
        self._control_request_handler = None
        requests = tuple(self._inflight_requests.values())
        self._inflight_requests.clear()
        for request in requests:
            request.cancel()
        await asyncio.gather(*requests, return_exceptions=True)
        task = self._reader_task
        self._reader_task = None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        error = ClaudeProcessExited(
            self._supervisor.returncode,
            self._supervisor.stderr_tail,
            self._supervisor.result_error_tail,
        )
        self._fail_pending(error)
        self._publish_failure(error)

    async def _read_messages(self) -> None:
        try:
            while True:
                envelope = await self._supervisor.receive()
                if envelope["type"] == "control_response":
                    response = envelope["response"]
                    request_id = (
                        response.get("request_id")
                        if isinstance(response, Mapping)
                        else None
                    )
                    if isinstance(request_id, str):
                        future = self._pending.pop(request_id, None)
                        if future is not None and not future.done():
                            future.set_result(envelope)
                    continue
                if envelope["type"] == "control_request":
                    request_id = envelope.get("request_id")
                    if not isinstance(request_id, str) or not request_id:
                        self._publish_failure(
                            ClaudeProtocolError(
                                "incoming control request requires a request_id"
                            )
                        )
                        continue
                    task = asyncio.create_task(
                        self._handle_control_request(envelope),
                        name=f"claude-code-control-{request_id}",
                    )
                    self._inflight_requests[request_id] = task
                    task.add_done_callback(
                        lambda completed, key=request_id: self._discard_request(
                            key, completed
                        )
                    )
                    continue
                if envelope["type"] == "control_cancel_request":
                    request_id = envelope.get("request_id")
                    if isinstance(request_id, str):
                        task = self._inflight_requests.pop(request_id, None)
                        if task is not None:
                            task.cancel()
                    continue
                self._message_sequence += 1
                sequence = self._message_sequence
                await self._route_business_message(envelope, sequence)
                if self._message_observer is not None:
                    self._message_observer(envelope)
        except Exception as error:  # noqa: BLE001
            self._fail_pending(error)
            self._publish_failure(error)

    async def _handle_control_request(self, envelope: JsonObject) -> None:
        request_id = envelope["request_id"]
        assert isinstance(request_id, str)
        try:
            handler = self._control_request_handler
            if handler is None:
                raise ClaudeProtocolError(
                    "Claude control request has no active turn handler"
                )
            response = await handler(envelope)
        except Exception as error:  # noqa: BLE001
            _LOGGER.warning(
                "Claude control request handler failed",
                extra={
                    "provider_request_id": request_id,
                    "error_kind": type(error).__name__,
                },
            )
            try:
                async with deadline_timeout(self._supervisor.timer, 10):
                    await self._supervisor.send(
                        {
                            "type": "control_response",
                            "response": {
                                "subtype": "error",
                                "request_id": request_id,
                                "error": (
                                    f"permission bridge failed: {type(error).__name__}"
                                ),
                            },
                        },
                    )
            except Exception as send_error:  # noqa: BLE001
                self._publish_failure(
                    send_error
                    if isinstance(send_error, ClaudeTransportError)
                    else ClaudeTransportError(
                        f"control response write failed: {type(send_error).__name__}"
                    )
                )
            return
        try:
            async with deadline_timeout(self._supervisor.timer, 10):
                await self._supervisor.send(
                    {
                        "type": "control_response",
                        "response": {
                            "subtype": "success",
                            "request_id": request_id,
                            "response": dict(response),
                        },
                    },
                )
        except Exception as error:  # noqa: BLE001
            self._publish_failure(
                error
                if isinstance(error, ClaudeTransportError)
                else ClaudeTransportError(
                    f"control response write failed: {type(error).__name__}"
                )
            )

    async def _route_business_message(
        self, envelope: JsonObject, sequence: int
    ) -> None:
        async with self._route_lock:
            origin = _origin_kind(envelope)
            if (
                envelope["type"] == "system"
                and envelope.get("subtype") == "task_notification"
            ):
                # Claude resumes a provider-owned cycle for completed background
                # work before emitting its task-notification result.
                self._provider_cycle.provider_wake_started()
            elif envelope["type"] == "user" and origin not in {None, "human"}:
                self._provider_cycle.provider_wake_started()
            inbox = self._turn_inbox
            if inbox is not None and self._provider_cycle.provider_wake_adopted:
                inbox.adopted_provider_wake = True
            if inbox is not None and sequence >= inbox.minimum_sequence:
                if envelope["type"] == "result" and (
                    origin in {None, "human"} or inbox.adopted_provider_wake
                ):
                    inbox.result = envelope
                else:
                    if envelope["type"] == "command_lifecycle" and envelope.get(
                        "state"
                    ) in {"completed", "cancelled", "discarded", "refused"}:
                        inbox.pending_human_messages.discard(
                            cast(str, envelope["command_uuid"])
                        )
                    inbox._messages.put_nowait(envelope)
                # Native lifecycle receipts settle coalesced and discarded inputs
                # without assuming one result per send. The route lock also
                # prevents accepting a steer behind an already released result.
                if inbox.result is not None and not inbox.pending_human_messages:
                    inbox.accepting_input = False
                    inbox._messages.put_nowait(inbox.result)
                    inbox.result = None
            if envelope["type"] == "result" and origin not in {None, "human"}:
                self._provider_cycle.provider_wake_finished()

    def _discard_request(self, request_id: str, completed: asyncio.Task[None]) -> None:
        if self._inflight_requests.get(request_id) is completed:
            self._inflight_requests.pop(request_id, None)
        if not completed.cancelled():
            completed.exception()

    def _publish_failure(self, error: BaseException) -> None:
        inbox = self._turn_inbox
        if inbox is not None:
            inbox._messages.put_nowait(error)

    def _fail_pending(self, error: BaseException) -> None:
        pending = tuple(self._pending.values())
        self._pending.clear()
        for future in pending:
            if not future.done():
                future.set_exception(error)

    def _next_request_id(self) -> str:
        self._request_counter += 1
        return f"req_{self._request_counter}_{secrets.token_hex(4)}"


def _user_envelope(text: str, message_id: str) -> JsonObject:
    return {
        "type": "user",
        "session_id": "default",
        "message": {"role": "user", "content": text},
        "parent_tool_use_id": None,
        "origin": {"kind": "human"},
        "uuid": message_id,
    }


def _origin_kind(envelope: Mapping[str, object]) -> str | None:
    origin = envelope.get("origin")
    if not isinstance(origin, Mapping):
        return None
    kind = origin.get("kind")
    return kind if isinstance(kind, str) and kind else None


__all__ = [
    "Client",
    "ProviderCycleState",
    "ProviderCycleStateMachine",
    "TurnInbox",
]
