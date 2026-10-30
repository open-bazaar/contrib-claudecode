from __future__ import annotations

from collections.abc import Mapping
from typing import cast

from bazaar_contrib_harness import HarnessFailure, HarnessFailureReason

type JsonObject = dict[str, object]


class ClaudeTransportError(RuntimeError):
    """The Claude CLI transport failed."""


class ClaudeProtocolError(ClaudeTransportError):
    """The Claude CLI emitted an envelope that cannot be routed safely."""


class ClaudeProcessNotRunning(ClaudeTransportError):
    """The Claude CLI process is unavailable."""


class ClaudeProcessExited(ClaudeTransportError):
    def __init__(
        self,
        returncode: int | None,
        stderr_tail: tuple[str, ...],
        result_error_tail: tuple[str, ...] = (),
    ) -> None:
        message = f"Claude CLI exited with code {returncode}"
        if stderr_tail:
            message = f"{message}: {stderr_tail[-1]}"
        elif result_error_tail:
            message = f"{message}: {result_error_tail[-1]}"
        super().__init__(message)
        self.returncode = returncode
        self.stderr_tail = stderr_tail
        self.result_error_tail = result_error_tail


class ClaudeControlError(ClaudeProtocolError):
    """A Claude CLI control request was rejected."""


def validate_envelope(value: Mapping[str, object]) -> JsonObject:
    envelope = dict(value)
    kind = envelope.get("type")
    if not isinstance(kind, str) or not kind:
        raise ClaudeProtocolError("Claude envelope requires a type")
    if kind == "control_response":
        response = envelope.get("response")
        if not isinstance(response, Mapping):
            raise ClaudeProtocolError("control response requires a response object")
        if not isinstance(response.get("request_id"), str):
            raise ClaudeProtocolError("control response requires a request_id")
    elif kind == "control_request":
        if not isinstance(envelope.get("request_id"), str):
            raise ClaudeProtocolError("control request requires a request_id")
        request = envelope.get("request")
        if not isinstance(request, Mapping) or not isinstance(
            request.get("subtype"), str
        ):
            raise ClaudeProtocolError("control request requires a subtype")
    elif kind == "assistant":
        if not isinstance(envelope.get("message"), Mapping):
            raise ClaudeProtocolError("assistant envelope requires a message")
    elif kind == "result":
        for field_name in (
            "subtype",
            "duration_ms",
            "duration_api_ms",
            "is_error",
            "num_turns",
            "session_id",
        ):
            if field_name not in envelope:
                raise ClaudeProtocolError(f"result envelope requires {field_name}")
    elif kind == "stream_event":
        if not isinstance(envelope.get("event"), Mapping):
            raise ClaudeProtocolError("stream event requires an event object")
    elif kind == "system":
        if not isinstance(envelope.get("subtype"), str):
            raise ClaudeProtocolError("system envelope requires a subtype")
    elif kind == "user":
        if not isinstance(envelope.get("message"), Mapping):
            raise ClaudeProtocolError("user envelope requires a message")
    elif kind in {"rate_limit_event", "conversation_reset"}:
        pass
    return cast(JsonObject, envelope)


def parse_control_response(envelope: Mapping[str, object]) -> JsonObject:
    message = validate_envelope(envelope)
    if message["type"] != "control_response":
        raise ClaudeProtocolError("expected a control response")
    response = cast(Mapping[str, object], message["response"])
    if response.get("subtype") == "error":
        error = response.get("error")
        raise ClaudeControlError(str(error or "Claude control request failed"))
    return dict(response)


def failure(error: BaseException) -> HarnessFailure:
    if isinstance(error, ClaudeControlError):
        reason = HarnessFailureReason.EXECUTION
    elif isinstance(error, ClaudeProtocolError):
        reason = HarnessFailureReason.PROTOCOL
    elif isinstance(error, TimeoutError):
        reason = HarnessFailureReason.TIMEOUT
    elif isinstance(error, ClaudeTransportError):
        reason = HarnessFailureReason.CONNECTION_LOST
    elif isinstance(error, FileNotFoundError):
        reason = HarnessFailureReason.UNAVAILABLE
    else:
        reason = HarnessFailureReason.EXECUTION
    return HarnessFailure(
        reason,
        str(error) or type(error).__name__,
        needs_reconciliation=reason
        in {
            HarnessFailureReason.TIMEOUT,
            HarnessFailureReason.CONNECTION_LOST,
        },
    )


__all__ = [
    "ClaudeControlError",
    "ClaudeProcessExited",
    "ClaudeProcessNotRunning",
    "ClaudeProtocolError",
    "ClaudeTransportError",
    "JsonObject",
    "parse_control_response",
    "validate_envelope",
]
