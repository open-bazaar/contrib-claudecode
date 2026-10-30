from __future__ import annotations

import json
import logging
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from enum import StrEnum
from time import time_ns
from typing import Self, cast

from bazaar_contrib_harness import (
    ContentDelta,
    ContentDeltaKind,
    ContextCompactionCompleted,
    HarnessEventPayload,
    HarnessFailure,
    HarnessFailureReason,
    HarnessOutput,
    TokenUsage,
    ToolCall,
    ToolCallCompleted,
    ToolCallDeltaKind,
    ToolCallFailed,
    ToolCallStarted,
    ToolCallTextDelta,
    TurnCancelled,
    TurnCompleted,
    TurnFailed,
    TurnStarted,
    UsageUpdated,
)
from pydantic import JsonValue, TypeAdapter

from .client import TurnInbox
from .protocol import ClaudeProtocolError, ClaudeTransportError, JsonObject

ClosedHandler = Callable[[], Awaitable[None]]
UnusableHandler = Callable[[BaseException], Awaitable[None]]
_LOGGER = logging.getLogger("bazaar_contrib_claude_code")
_JSON_VALUE_ADAPTER = TypeAdapter(JsonValue)


class TurnState(StrEnum):
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


class TurnEventStream:
    """Normalize one persistent Claude stream into provider-neutral turn events."""

    def __init__(
        self,
        inbox: TurnInbox,
        *,
        session_id: str,
        turn_id: str,
        provider_thread_id: str,
        claude_version: tuple[int, int, int],
        on_closed: ClosedHandler,
        on_unusable: UnusableHandler,
        initial_error: BaseException | None = None,
        initial_error_state: TurnState = TurnState.UNKNOWN,
        initial_error_kind: str = "provider_unknown",
    ) -> None:
        self._inbox = inbox
        self._session_id = session_id
        self._turn_id = turn_id
        self._provider_thread_id = provider_thread_id
        self._claude_version = claude_version
        self._on_closed = on_closed
        self._on_unusable = on_unusable
        self._initial_error = initial_error
        self._initial_error_state = initial_error_state
        self._initial_error_kind = initial_error_kind
        self._initial_emitted = False
        self._terminal_emitted = False
        self._closed = False
        self._closed_callback_called = False
        self._pending: deque[HarnessOutput] = deque()
        self._tool_names: dict[str, str] = {}
        self._block_stream_ids: dict[int, str] = {}
        self._raw: JsonObject = {}

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> HarnessOutput:
        if self._closed:
            raise StopAsyncIteration
        if self._pending:
            return self._pending.popleft()
        if self._terminal_emitted:
            raise StopAsyncIteration
        if not self._initial_emitted:
            self._initial_emitted = True
            if self._initial_error is not None:
                await self._on_unusable(self._initial_error)
                return await self._terminal_event(
                    self._initial_error_state,
                    event_name="claudecode.turn.start.unknown",
                    error_kind=self._initial_error_kind,
                    error_message=str(self._initial_error)
                    or type(self._initial_error).__name__,
                )
            return self._runtime_event(
                event_name="claudecode.turn.started",
                state=TurnState.STARTED,
                metadata={"provider_thread_id": self._provider_thread_id},
            )
        while not self._closed:
            try:
                message = await self._inbox.receive()
                self._raw = message
                items = await self._map_message(message)
            except (ClaudeProtocolError, TypeError, ValueError) as error:
                await self._on_unusable(error)
                return await self._terminal_event(
                    TurnState.UNKNOWN,
                    event_name="claudecode.turn.protocol.unknown",
                    error_kind="protocol",
                    error_message=str(error) or type(error).__name__,
                )
            except ClaudeTransportError as error:
                await self._on_unusable(error)
                return await self._terminal_event(
                    TurnState.UNKNOWN,
                    event_name="claudecode.turn.transport.unknown",
                    error_kind="provider_unknown",
                    error_message=str(error) or type(error).__name__,
                )
            self._pending.extend(items)
            if self._pending:
                return self._pending.popleft()
        raise StopAsyncIteration

    async def aclose(self) -> None:
        self._closed = True
        if not self._terminal_emitted:
            await self._on_unusable(
                RuntimeError("Claude turn stream closed before a terminal result")
            )
        await self._call_closed()

    async def _map_message(self, message: JsonObject) -> tuple[HarnessOutput, ...]:
        kind = message["type"]
        session_id = message.get("session_id")
        if kind in {
            "user",
            "assistant",
            "system",
            "result",
            "stream_event",
            "conversation_reset",
        } and not isinstance(session_id, str):
            raise ClaudeProtocolError(f"{kind} envelope requires a session_id")
        if session_id is not None and session_id != self._provider_thread_id:
            raise ClaudeProtocolError("Claude envelope belongs to another session")
        if kind == "result":
            return await self._map_result(message)
        if kind == "stream_event":
            item = self._map_stream_event(message)
            return (item,) if item is not None else ()
        if kind == "assistant":
            return self._map_assistant(message)
        if kind == "user":
            return self._map_user(message)
        if kind == "system":
            return self._map_system(message)
        if kind == "rate_limit_event":
            return ()
        if kind == "conversation_reset":
            await self._on_unusable(
                ClaudeProtocolError("Claude conversation was reset")
            )
            return (
                await self._terminal_event(
                    TurnState.UNKNOWN,
                    event_name="claudecode.turn.conversation_reset",
                    error_kind="provider_unknown",
                    error_message="Claude conversation was reset",
                ),
            )
        _LOGGER.debug(
            "skipping unknown Claude envelope type", extra={"provider_type": kind}
        )
        return ()

    def _map_user(self, message: JsonObject) -> tuple[HarnessOutput, ...]:
        """Read a user envelope as the tool results it carries."""

        raw_message = message.get("message")
        content = (
            raw_message.get("content") if isinstance(raw_message, Mapping) else None
        )
        items = tuple(
            self._tool_result_event(block, message)
            for block in (content if isinstance(content, list) else ())
            if isinstance(block, Mapping) and block.get("type") == "tool_result"
        )
        if items:
            return items
        tool_result = message.get("tool_use_result")
        if tool_result is None:
            return ()
        item = self._text_delta(
            _stream_id(message),
            ToolCallDeltaKind.PROGRESS,
            _content_text(tool_result),
        )
        return (item,) if item is not None else ()

    def _tool_result_event(
        self, block: Mapping[str, object], message: JsonObject
    ) -> HarnessOutput:
        """Turn one tool_result block into the call it finishes."""

        call_id = _text(block.get("tool_use_id"))
        if call_id is None:
            raise ClaudeProtocolError("tool result requires a tool_use_id")
        is_error = block.get("is_error")
        if is_error is not None and not isinstance(is_error, bool):
            raise ClaudeProtocolError("tool result is_error must be a boolean")
        try:
            output = _JSON_VALUE_ADAPTER.validate_python(
                block.get("content"), strict=True
            )
        except ValueError as error:
            raise ClaudeProtocolError(
                "tool result content must be a JSON value"
            ) from error
        call = ToolCall(
            call_id=call_id,
            name=self._tool_names.pop(call_id, call_id),
            parent_call_id=_text(message.get("parent_tool_use_id")),
            output=output,
        )
        if is_error is True:
            return self._output_event(
                ToolCallFailed(call=call, error_message=_content_text(output))
            )
        return self._output_event(ToolCallCompleted(call=call))

    def _map_system(self, message: JsonObject) -> tuple[HarnessOutput, ...]:
        """Read a system envelope, which mostly narrates rather than reports."""

        subtype = message.get("subtype")
        if subtype == "init":
            self._validate_cli_version(message)
            return ()
        if subtype == "compact_boundary":
            return (
                self._output_event(
                    ContextCompactionCompleted(compaction_id=_stream_id(message))
                ),
            )
        if subtype == "task_started":
            call_id = _text(message.get("task_id")) or _stream_id(message)
            if call_id is None:
                return ()
            return (
                self._output_event(
                    ToolCallStarted(
                        call=ToolCall(
                            call_id=call_id,
                            name="task",
                            parent_call_id=_text(message.get("parent_tool_use_id")),
                            input=_text(message.get("summary")),
                        )
                    )
                ),
            )
        if subtype in {"tool_progress", "task_notification", "task_updated"}:
            item = self._text_delta(
                _text(message.get("task_id")) or _stream_id(message),
                ToolCallDeltaKind.PROGRESS,
                _text(message.get("summary")),
            )
            return (item,) if item is not None else ()
        return ()

    async def _map_result(self, message: JsonObject) -> tuple[HarnessOutput, ...]:
        if not isinstance(message.get("subtype"), str) or not isinstance(
            message.get("is_error"), bool
        ):
            raise ClaudeProtocolError("Claude result fields are invalid")
        for field_name in ("duration_ms", "duration_api_ms", "num_turns"):
            field_value = message.get(field_name)
            if not isinstance(field_value, int) or isinstance(field_value, bool):
                raise ClaudeProtocolError(f"Claude result {field_name} is invalid")
        origin = message.get("origin")
        # A foreground turn opened during a provider-owned wake adopts that cycle,
        # so its non-human result is also the foreground terminal.
        if (
            isinstance(origin, Mapping)
            and origin.get("kind") != "human"
            and not self._inbox.adopted_provider_wake
        ):
            return ()
        metadata = _result_metadata(message)
        items: list[HarnessOutput] = []
        # `usage` is this turn's; `modelUsage` and `total_cost_usd` are the
        # session's running totals, which is what a usage event carries
        usage = message.get("usage")
        model_usage = message.get("modelUsage")
        cost = message.get("total_cost_usd")
        if usage is not None and not isinstance(usage, Mapping):
            raise ClaudeProtocolError("Claude result usage must be an object")
        if model_usage is not None and not isinstance(model_usage, Mapping):
            raise ClaudeProtocolError("Claude result modelUsage must be an object")
        if cost is not None and (
            not isinstance(cost, (int, float)) or isinstance(cost, bool)
        ):
            raise ClaudeProtocolError("Claude result total_cost_usd must be numeric")
        if usage is not None or model_usage is not None or cost is not None:
            last = _token_usage(usage) if usage is not None else None
            items.append(
                self._output_event(
                    UsageUpdated(
                        # the session's total is only what modelUsage says;
                        # one turn's usage is not it and is not passed off as it
                        total=(
                            _session_usage(model_usage)
                            if model_usage is not None
                            else TokenUsage()
                        ),
                        last=last,
                        cost_usd=float(cost) if cost is not None else None,
                    )
                )
            )
        if message.get("deferred_tool_use"):
            items.append(
                await self._terminal_event(
                    TurnState.FAILED,
                    event_name="claudecode.turn.failed",
                    error_kind="provider_deferred",
                    error_message="Claude deferred a tool without executing it",
                    metadata=metadata,
                )
            )
            return tuple(items)
        if (
            message.get("subtype") != "success"
            or message.get("is_error") is True
            or message.get("errors")
            or message.get("api_error_status")
        ):
            items.append(
                await self._terminal_event(
                    TurnState.FAILED,
                    event_name="claudecode.turn.failed",
                    error_kind="provider_failed",
                    error_message=_result_error(message),
                    metadata=metadata,
                )
            )
            return tuple(items)
        items.append(
            await self._terminal_event(
                TurnState.COMPLETED,
                event_name="claudecode.turn.completed",
                metadata=metadata,
            )
        )
        return tuple(items)

    def _map_stream_event(self, message: JsonObject) -> HarnessOutput | None:
        raw_event = message.get("event")
        if not isinstance(raw_event, Mapping):
            raise ClaudeProtocolError("stream event requires an event object")
        event_type = raw_event.get("type")
        if event_type == "content_block_delta":
            delta = raw_event.get("delta")
            if not isinstance(delta, Mapping):
                raise ClaudeProtocolError("content block delta requires delta")
            delta_type = delta.get("type")
            content = delta.get("text")
            index = raw_event.get("index")
            if delta_type == "input_json_delta":
                content = delta.get("partial_json")
                stream_id = (
                    self._block_stream_ids.get(index)
                    if isinstance(index, int)
                    else None
                ) or _stream_id(message)
                return self._text_delta(
                    stream_id,
                    ToolCallDeltaKind.INPUT,
                    content if isinstance(content, str) else None,
                )
            elif delta_type == "thinking_delta":
                content = delta.get("thinking")
                content_kind = ContentDeltaKind.REASONING_TEXT
            elif delta_type == "summary_text_delta":
                content_kind = ContentDeltaKind.REASONING_SUMMARY
            elif delta_type == "signature_delta":
                return None
            elif delta_type == "text_delta":
                content_kind = ContentDeltaKind.AGENT_MESSAGE
            else:
                return None
            if not isinstance(content, str):
                return None
            return self._output_event(
                ContentDelta(
                    kind=content_kind,
                    text=content,
                    index=index if isinstance(index, int) else None,
                )
            )
        if event_type == "content_block_start":
            block = raw_event.get("content_block")
            if not isinstance(block, Mapping) or block.get("type") != "tool_use":
                return None
            index = raw_event.get("index")
            block_id = _text(block.get("id"))
            if isinstance(index, int) and block_id is not None:
                self._block_stream_ids[index] = block_id
            return None
        if event_type == "content_block_stop":
            index = raw_event.get("index")
            if isinstance(index, int):
                self._block_stream_ids.pop(index, None)
            return None
        if event_type == "message_delta":
            return None
        return None

    def _map_assistant(self, message: JsonObject) -> tuple[HarnessOutput, ...]:
        raw_message = message.get("message")
        if not isinstance(raw_message, Mapping):
            raise ClaudeProtocolError("assistant envelope requires a message")
        content = raw_message.get("content")
        if not isinstance(content, list):
            raise ClaudeProtocolError("assistant message content must be a list")
        items: list[HarnessOutput] = []
        for block in content:
            if not isinstance(block, Mapping):
                continue
            block_type = block.get("type")
            if block_type == "tool_use":
                call_id = _text(block.get("id"))
                name = _text(block.get("name"))
                if call_id is None or name is None:
                    raise ClaudeProtocolError("tool use requires id and name")
                try:
                    input = _JSON_VALUE_ADAPTER.validate_python(
                        block.get("input"), strict=True
                    )
                except ValueError as error:
                    raise ClaudeProtocolError(
                        "tool use input must be a JSON value"
                    ) from error
                self._tool_names[call_id] = name
                items.append(
                    self._output_event(
                        ToolCallStarted(
                            call=ToolCall(
                                call_id=call_id,
                                name=name,
                                parent_call_id=_text(message.get("parent_tool_use_id")),
                                input=input,
                            )
                        )
                    )
                )
        return tuple(items)

    def _validate_cli_version(self, message: JsonObject) -> None:
        actual = message.get("claude_code_version")
        expected = ".".join(str(part) for part in self._claude_version)
        if actual != expected:
            raise ClaudeProtocolError(
                f"Claude initialization version mismatch: expected {expected}, got {actual}"
            )

    def _runtime_event(
        self,
        *,
        event_name: str,
        state: TurnState,
        error_kind: str | None = None,
        error_message: str | None = None,
        metadata: Mapping[str, JsonValue] | None = None,
    ) -> HarnessOutput:
        if state is TurnState.STARTED:
            payload = TurnStarted(event_name=event_name, metadata=dict(metadata or {}))
        elif state is TurnState.COMPLETED:
            payload = TurnCompleted(
                event_name=event_name, metadata=dict(metadata or {})
            )
        elif state is TurnState.CANCELLED:
            payload = TurnCancelled(
                event_name=event_name, metadata=dict(metadata or {})
            )
        else:
            reason = (
                HarnessFailureReason.PROTOCOL
                if error_kind == "protocol"
                else HarnessFailureReason.CONNECTION_LOST
                if state is TurnState.UNKNOWN
                else HarnessFailureReason.EXECUTION
            )
            payload = TurnFailed(
                event_name=event_name,
                failure=HarnessFailure(
                    reason,
                    error_message or error_kind or "Claude turn failed",
                    needs_reconciliation=state is TurnState.UNKNOWN,
                    code=error_kind,
                ),
                metadata=dict(metadata or {}),
            )
        return self._output_event(payload)

    def _output_event(self, payload: HarnessEventPayload) -> HarnessOutput:
        kind = self._raw.get("type")
        subtype = self._raw.get("subtype")
        return HarnessOutput(
            session_id=self._session_id,
            turn_id=self._turn_id,
            payload=payload,
            provider="claudecode",
            provider_event=f"{kind}.{subtype}"
            if subtype
            else str(kind)
            if kind
            else None,
            provider_session_id=self._provider_thread_id,
            occurred_at_ms=time_ns() // 1_000_000,
            raw=cast(JsonValue, self._raw),
        )

    def _text_delta(
        self,
        call_id: str | None,
        kind: ToolCallDeltaKind,
        text: str | None,
    ) -> HarnessOutput | None:
        if call_id is None or text is None:
            return None
        return self._output_event(
            ToolCallTextDelta(call_id=call_id, kind=kind, text=text)
        )

    async def _terminal_event(
        self,
        state: TurnState,
        *,
        event_name: str,
        error_kind: str | None = None,
        error_message: str | None = None,
        metadata: Mapping[str, JsonValue] | None = None,
    ) -> HarnessOutput:
        self._terminal_emitted = True
        self._tool_names.clear()
        self._block_stream_ids.clear()
        event = self._runtime_event(
            event_name=event_name,
            state=state,
            error_kind=error_kind,
            error_message=error_message,
            metadata=metadata,
        )
        await self._call_closed()
        return event

    async def _call_closed(self) -> None:
        if self._closed_callback_called:
            return
        self._closed_callback_called = True
        await self._on_closed()


def _stream_id(message: Mapping[str, object]) -> str | None:
    return _text(message.get("parent_tool_use_id")) or _text(message.get("uuid"))


def _token_usage(usage: Mapping[str, object]) -> TokenUsage:
    """One turn's usage as Claude reports it; it never says the total, which
    is what its parts add up to."""

    counts = _counts(
        usage,
        input_tokens="input_tokens",
        cached_input_tokens="cache_read_input_tokens",
        cache_write_input_tokens="cache_creation_input_tokens",
        output_tokens="output_tokens",
        reasoning_output_tokens="reasoning_output_tokens",
    )
    return TokenUsage(**counts, total_tokens=_sum_of(counts))


def _session_usage(model_usage: Mapping[str, object]) -> TokenUsage:
    """The session's usage so far, which Claude keeps per model."""

    totals: dict[str, int | None] = {}
    for entry in model_usage.values():
        if not isinstance(entry, Mapping):
            raise ClaudeProtocolError(
                "Claude result modelUsage entries must be objects"
            )
        counts = _counts(
            entry,
            input_tokens="inputTokens",
            cached_input_tokens="cacheReadInputTokens",
            cache_write_input_tokens="cacheCreationInputTokens",
            output_tokens="outputTokens",
            reasoning_output_tokens="thinkingTokens",
        )
        for name, count in counts.items():
            if count is not None:
                totals[name] = (totals.get(name) or 0) + count
    return TokenUsage(**totals, total_tokens=_sum_of(totals))


def _counts(source: Mapping[str, object], **names: str) -> dict[str, int | None]:
    counts: dict[str, int | None] = {}
    for field_name, key in names.items():
        value = source.get(key)
        if value is not None and (
            not isinstance(value, int) or isinstance(value, bool)
        ):
            raise ClaudeProtocolError(f"Claude result usage {key} must be an integer")
        counts[field_name] = value
    return counts


def _sum_of(counts: Mapping[str, int | None]) -> int | None:
    """Every token the model read or wrote; nothing when nothing was counted."""

    known = [
        count
        for name, count in counts.items()
        if count is not None and name != "reasoning_output_tokens"
    ]
    return sum(known) if known else None


def _result_metadata(message: Mapping[str, object]) -> dict[str, JsonValue]:
    metadata: dict[str, JsonValue] = {
        "provider_thread_id": cast(JsonValue, message["session_id"]),
        "provider_subtype": cast(JsonValue, message["subtype"]),
        "duration_ms": cast(JsonValue, message["duration_ms"]),
        "duration_api_ms": cast(JsonValue, message["duration_api_ms"]),
        "num_turns": cast(JsonValue, message["num_turns"]),
    }
    for field_name in (
        "stop_reason",
        "total_cost_usd",
        "usage",
        "modelUsage",
        "terminal_reason",
        "origin",
    ):
        value = message.get(field_name)
        if value is not None:
            metadata[field_name] = cast(JsonValue, value)
    permission_denials = message.get("permission_denials")
    if isinstance(permission_denials, list):
        metadata["permission_denial_count"] = len(permission_denials)
    return metadata


def _result_error(message: Mapping[str, object]) -> str | None:
    errors = message.get("errors")
    if isinstance(errors, list) and errors:
        return "; ".join(str(item) for item in errors)
    result = message.get("result")
    if isinstance(result, str) and result:
        return result
    return None


def _content_text(value: object) -> str | None:
    if isinstance(value, str):
        return value
    if value is not None:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return None


def _text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


__all__ = ["TurnEventStream"]
